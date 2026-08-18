package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
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

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

                    //3.解析消息中的消息订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.如果获取成功，可以下单
                    if (!handleVoucherOrder(voucherOrder)) {
                        throw new IllegalStateException("订单处理失败");
                    }
                    stringRedisTemplate.opsForStream().acknowledge(queueName, STREAM_GROUP, record.getId());
                }catch (Exception e){
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

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

                    //3.解析消息中的消息订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.如果获取成功，可以下单
                    if (!handleVoucherOrder(voucherOrder)) {
                        throw new IllegalStateException("订单处理失败");
                    }
                    stringRedisTemplate.opsForStream().acknowledge(queueName, STREAM_GROUP, record.getId());
                }catch (Exception e){
                    log.error("处理pending-list订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }



    private boolean handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败 ,返回失败
            log.error("不允许重复下单");
            return false;
        }
        try {

            return proxy.createVoucherOrder(voucherOrder);
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

        return Result.ok(orderId);
    }





    @Transactional
    public boolean createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();
        //查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //判断是否存在
        if (count>0) {
            log.error("用户已经购买了一次");
            return true;
        }

        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1").eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock",0).update();
        if(!success){
            //扣减失败
            log.error("库存不足");
            return false;
        }

        return save(voucherOrder);

    }
}
