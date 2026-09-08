package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.OrderHandleResult;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;

import static com.hmdp.utils.RedisConstants.*;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataAccessException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 秒杀订单服务
 *
 * 异步下单链路：
 * 1. 请求线程执行 Lua 脚本：校验库存/防重 -> 扣减 Redis 库存 -> 占位 -> 发送订单消息到 stream.orders
 * 2. 消费线程（g1/c1）读取消息，在 DB 事务中扣减 DB 库存并创建订单
 * 3. 失败处理策略（方案A）：
 *    - 可重试失败（分布式锁竞争、DB 临时异常）：消息保留在 pending-list，累计重试 MAX_PENDING_RETRY 次
 *    - 永久失败（DB 库存不足）：直接转死信队列 stream.orders.dlq
 *    - 死信消息由 SeckillOrderReconcileTask 定时对账：若订单确实未创建，则回补 Redis 库存并释放占位
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    @Lazy
    private IVoucherOrderService proxy;

    private static final String STREAM_ORDERS = "stream.orders";
    private static final String STREAM_GROUP = "g1";
    private static final String STREAM_CONSUMER = "c1";
    /** 死信队列：重试超过上限的订单消息 */
    public static final String STREAM_ORDERS_DLQ = "stream.orders.dlq";
    /** pending-list 最大重试次数 */
    private static final int MAX_PENDING_RETRY = 3;
    /** 消息ID -> 已重试次数（单线程消费者，本地计数即可；重启后重新计数可接受） */
    private final Map<String, Integer> retryCounts = new ConcurrentHashMap<>();

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //单线程的线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        createStreamGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private void createStreamGroup() {
        try {
            stringRedisTemplate.execute((RedisCallback<String>) connection -> connection.xGroupCreate(
                    STREAM_ORDERS.getBytes(StandardCharsets.UTF_8), STREAM_GROUP, ReadOffset.from("0"), true));
        } catch (DataAccessException e) {
            String message = e.getMessage();
            if (message == null || !message.contains("BUSYGROUP")) {
                throw e;
            }
        }
    }

    private class VoucherOrderHandler implements Runnable{
        String queueName = STREAM_ORDERS;
        @Override
        public void run() {
            while(true){
                try{
                   //1.获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP, STREAM_CONSUMER),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1 如果获取失败 说明没有消息 继续下一次循环
                        continue;
                    }

                    //3.处理消息（内部完成 ACK / 转死信 / 保留待重试）
                    MapRecord<String, Object, Object> record = list.get(0);
                    processRecord(record);
                }catch (Exception e){
                    //4.读取或处理异常：进入 pending-list 兜底重试
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        /**
         * 兜底处理 pending-list 中的失败消息。
         * 每条消息累计重试，达到 MAX_PENDING_RETRY 次仍失败则转死信队列并 ACK，
         * 由对账任务负责回补库存，避免死循环重试。
         */
        private void handlePendingList() {
            while(true){
                try{
                    //1.获取pending-list中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(STREAM_GROUP, STREAM_CONSUMER),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1 如果获取失败 说明pendinglist没有异常消息 结束循环
                        break;
                    }

                    //3.处理消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    String msgId = record.getId().getValue();
                    try {
                        if (processRecord(record)) {
                            //处理成功（ACK或转死信），清除重试计数
                            retryCounts.remove(msgId);
                            continue;
                        }
                    } catch (Exception e) {
                        log.error("处理pending-list订单异常: msgId={}", msgId, e);
                    }

                    //4.处理失败（可重试型）：累计重试次数
                    int retries = retryCounts.merge(msgId, 1, Integer::sum);
                    if (retries >= MAX_PENDING_RETRY) {
                        //5.重试超限：转死信队列，并 ACK 原消息（否则消息永远滞留在 pending-list）
                        sendToDlq(record, "重试" + retries + "次后仍失败");
                        try {
                            stringRedisTemplate.opsForStream().acknowledge(queueName, STREAM_GROUP, record.getId());
                        } catch (Exception ackEx) {
                            log.error("ACK死信消息失败: msgId={}", msgId, ackEx);
                        }
                        retryCounts.remove(msgId);
                        log.error("订单消息累计重试{}次仍失败，已转死信队列: msgId={}", retries, msgId);
                    } else {
                        log.warn("订单消息处理失败，第{}/{}次重试等待: msgId={}", retries, MAX_PENDING_RETRY, msgId);
                    }
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }catch (Exception e){
                    log.error("处理pending-list异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        /**
         * 处理一条订单消息
         * @return true = 消息已被终结（ACK 或转死信）；false = 可重试失败，消息保留在 pending-list
         */
        private boolean processRecord(MapRecord<String, Object, Object> record) {
            //1.解析消息中的订单信息
            Map<Object, Object> values = record.getValue();
            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
            //2.执行下单
            OrderHandleResult result = handleVoucherOrder(voucherOrder);
            switch (result) {
                case SUCCESS:
                    //3.下单成功，ACK
                    stringRedisTemplate.opsForStream().acknowledge(STREAM_ORDERS, STREAM_GROUP, record.getId());
                    return true;
                case FATAL:
                    //4.永久失败（如DB库存不足）：转死信队列，由对账任务回补Redis库存；ACK 避免死循环
                    sendToDlq(record, "扣减DB库存失败(永久性失败)");
                    stringRedisTemplate.opsForStream().acknowledge(STREAM_ORDERS, STREAM_GROUP, record.getId());
                    return true;
                case RETRYABLE:
                default:
                    //5.可重试失败：不 ACK，消息保留在 pending-list
                    return false;
            }
        }

        /**
         * 将订单消息写入死信队列（保留原始消息字段 + 失败原因 + 原消息ID）
         */
        private void sendToDlq(MapRecord<String, Object, Object> record, String reason) {
            try {
                Map<Object, Object> values = record.getValue();
                Map<String, String> fields = new HashMap<>();
                fields.put("userId", String.valueOf(values.get("userId")));
                fields.put("voucherId", String.valueOf(values.get("voucherId")));
                fields.put("id", String.valueOf(values.get("id")));
                fields.put("reason", reason);
                fields.put("originalId", record.getId().getValue());
                stringRedisTemplate.opsForStream().add(STREAM_ORDERS_DLQ, fields);
                log.error("订单消息进入死信队列: originalId={}, reason={}", record.getId().getValue(), reason);
            } catch (Exception e) {
                //写死信失败时消息仍在 pending-list，下次重试仍有机会
                log.error("写入死信队列失败: msgId={}", record.getId().getValue(), e);
            }
        }
    }

    /**
     * 执行下单：加 Redisson 锁串行化同一用户的订单，区分可重试/永久失败
     */
    private OrderHandleResult handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败，可稍后重试
            log.warn("获取分布式锁失败，稍后重试: userId={}", userId);
            return OrderHandleResult.RETRYABLE;
        }
        try {
            return proxy.createVoucherOrder(voucherOrder);
        } catch (Exception e) {
            //DB 临时异常，可稍后重试
            log.error("创建订单异常，稍后重试: userId={}", userId, e);
            return OrderHandleResult.RETRYABLE;
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),String.valueOf(orderId)
        );

        //2.判断结果是否为0
        int r = result.intValue();
        //2.1 不为0  代表没有购买资格
        if (r!=0) {
            //2.1 不为0 代表没有购买资格
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }

        //3.受理成功：写入"处理中"状态，供前端轮询查询下单结果（TTL 过期后以 DB 为准）
        stringRedisTemplate.opsForValue().set(
                ORDER_STATUS_KEY + orderId, ORDER_STATUS_CREATING, ORDER_STATUS_TTL, TimeUnit.MINUTES);

        return Result.ok(orderId);
    }

    /**
     * 在 DB 事务中创建订单：一人一单校验 -> 乐观锁扣减库存 -> 插入订单。
     * 任一失败整个事务回滚，不会出现"库存扣了订单没建"的不一致。
     *
     * 订单状态（Redis）与订单创建（DB）不放在同一原子操作中：
     * - SUCCESS 状态在事务【提交成功后】通过 afterCommit 写入，保证"看到成功=订单已在DB"；
     * - 事务回滚时 afterCommit 不执行，状态停留在 CREATING，由查询接口回源 DB 兜底；
     * - 状态写失败也不影响主流程（saveOrderStatus 内部捕获），最终以 DB 为真相。
     */
    @Transactional
    public OrderHandleResult createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();
        //查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //判断是否存在
        if (count>0) {
            //已购买：幂等成功（消息可 ACK，但不能重复建单）
            log.warn("用户已购买过一次，幂等返回: userId={}, voucherId={}", userId, voucherOrder.getVoucherId());
            registerSuccessAfterCommit(voucherOrder.getId());
            return OrderHandleResult.SUCCESS;
        }

        //扣减库存（乐观锁：stock > 0）
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1").eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock",0).update();
        if(!success){
            //DB 库存不足：永久失败，转死信队列由对账任务回补 Redis 库存
            log.error("扣减DB库存失败(永久性): voucherId={}", voucherOrder.getVoucherId());
            //本分支事务无任何写入，提交必然成功，直接写失败状态（用户可立即感知，无需等对账）
            saveOrderStatus(voucherOrder.getId(), ORDER_STATUS_FAILED);
            return OrderHandleResult.FATAL;
        }

        //插入订单（订单状态：1=未支付，支付/核销流程后续基于该字段扩展）
        voucherOrder.setStatus(1);
        if (!save(voucherOrder)) {
            //抛异常触发事务回滚，连同上面的库存扣减一起回滚
            throw new IllegalStateException("订单保存失败");
        }
        //事务提交成功后才写 SUCCESS 状态
        registerSuccessAfterCommit(voucherOrder.getId());
        return OrderHandleResult.SUCCESS;
    }

    /**
     * 注册事务提交成功后的回调：订单已落库才写 SUCCESS 状态（提交后通知，非同一原子操作）
     */
    private void registerSuccessAfterCommit(Long orderId) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                saveOrderStatus(orderId, ORDER_STATUS_SUCCESS);
            }
        });
    }

    /**
     * 写入订单状态到 Redis（String 类型，带 TTL 的通知窗口）
     */
    private void saveOrderStatus(Long orderId, String status) {
        try {
            stringRedisTemplate.opsForValue().set(
                    ORDER_STATUS_KEY + orderId, status, ORDER_STATUS_TTL, TimeUnit.MINUTES);
            log.debug("订单状态已写入Redis: orderId={}, status={}", orderId, status);
        } catch (Exception e) {
            //状态写失败不阻塞主流程，查询接口会回源 DB 兜底
            log.error("写入订单状态失败: orderId={}, status={}", orderId, status, e);
        }
    }

    /**
     * 查询异步下单结果：Redis 状态优先（毫秒级，扛住前端轮询），key 缺失/过期时回源 DB 兜底。
     * 返回 SUCCESS 时附带完整订单；FAILED 返回失败提示；处理中返回 CREATING 语义。
     */
    @Override
    public Result queryOrderStatus(Long orderId) {
        //1.先查 Redis 状态（通知窗口内）
        String status = stringRedisTemplate.opsForValue().get(ORDER_STATUS_KEY + orderId);
        if (ORDER_STATUS_SUCCESS.equals(status)) {
            //2.下单成功：返回完整订单
            VoucherOrder order = getById(orderId);
            if (order != null) {
                return Result.ok(order);
            }
            //极端情况：状态写了但订单查询不到，落到 DB 兜底逻辑继续
        } else if (ORDER_STATUS_FAILED.equals(status)) {
            //3.永久失败：告知用户（库存已由对账任务回补）
            return Result.fail("订单创建失败，库存已退回");
        }

        //4.CREATING 或 key 已过期：回源 DB（真相在 DB，状态 key 只是通知窗口）
        VoucherOrder order = getById(orderId);
        if (order != null) {
            return Result.ok(order);
        }
        return Result.ok("处理中");
    }
}
