package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.Message;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hmdp.mq.MqConstants.*;
import static com.hmdp.utils.RedisConstants.ORDER_STATUS_FAILED;
import static com.hmdp.utils.RedisConstants.ORDER_STATUS_KEY;
import static com.hmdp.utils.RedisConstants.ORDER_STATUS_TTL;

@Slf4j
@RocketMQTransactionListener
public class BusinessTransactionListener implements RocketMQLocalTransactionListener {

    private static final DefaultRedisScript<Long> SECKILL_MQ_SCRIPT;

    static {
        SECKILL_MQ_SCRIPT = new DefaultRedisScript<>();
        SECKILL_MQ_SCRIPT.setLocation(new ClassPathResource("seckill_mq.lua"));
        SECKILL_MQ_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ShopMqTransactionService shopTransactionService;

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        if (!(arg instanceof MqTransactionContext)) {
            log.error("RocketMQ本地事务缺少上下文");
            return RocketMQLocalTransactionState.ROLLBACK;
        }
        MqTransactionContext context = (MqTransactionContext) arg;
        context.setLocalTransactionStarted(true);
        MqBusinessMessage businessMessage = context.getMessage();
        if (TYPE_SECKILL_ORDER.equals(businessMessage.getType())) {
            return executeSeckill(context);
        }
        if (TYPE_SHOP_UPDATE.equals(businessMessage.getType())) {
            return executeShopUpdate(context);
        }
        context.setErrorMessage("未知事务消息类型");
        return RocketMQLocalTransactionState.ROLLBACK;
    }

    private RocketMQLocalTransactionState executeSeckill(MqTransactionContext context) {
        VoucherOrder order = context.getMessage().getOrder();
        String reservationKey = RESERVATION_KEY_PREFIX + order.getId();
        try {
            stringRedisTemplate.opsForValue().set(reservationKey, "PROCESSING", 60, TimeUnit.SECONDS);
            Long result = stringRedisTemplate.execute(
                    SECKILL_MQ_SCRIPT,
                    Collections.emptyList(),
                    order.getVoucherId().toString(),
                    order.getUserId().toString(),
                    order.getId().toString());
            if (result == null) {
                throw new IllegalStateException("秒杀Lua脚本未返回结果");
            }
            context.setBusinessResult(result.intValue());
            if (result == 0) {
                context.setLocalTransactionCommitted(true);
                return RocketMQLocalTransactionState.COMMIT;
            }
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            context.setErrorMessage(e.getMessage());
            log.error("秒杀Redis预扣结果未知，等待RocketMQ回查: orderId={}", order.getId(), e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    private RocketMQLocalTransactionState executeShopUpdate(MqTransactionContext context) {
        String transactionId = context.getMessage().getTransactionId();
        try {
            shopTransactionService.updateShopAndCommit(transactionId, context.getMessage().getShop());
            context.setLocalTransactionCommitted(true);
            return RocketMQLocalTransactionState.COMMIT;
        } catch (Exception e) {
            context.setErrorMessage(e.getMessage());
            log.error("店铺本地事务回滚: transactionId={}", transactionId, e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        MqBusinessMessage businessMessage;
        try {
            businessMessage = parse(message);
        } catch (Exception e) {
            log.error("RocketMQ事务消息解析失败，回滚Half消息", e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
        if (TYPE_SHOP_UPDATE.equals(businessMessage.getType())) {
            return checkShopTransaction(businessMessage.getTransactionId());
        }
        if (TYPE_SECKILL_ORDER.equals(businessMessage.getType())) {
            return checkSeckillTransaction(businessMessage.getOrder());
        }
        return RocketMQLocalTransactionState.ROLLBACK;
    }

    private RocketMQLocalTransactionState checkShopTransaction(String transactionId) {
        try {
            String status = shopTransactionService.findStatus(transactionId);
            if (TX_COMMITTED.equals(status)) {
                return RocketMQLocalTransactionState.COMMIT;
            }
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            log.error("查询店铺本地事务状态失败: transactionId={}", transactionId, e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    private RocketMQLocalTransactionState checkSeckillTransaction(VoucherOrder order) {
        boolean databaseAvailable = true;
        try {
            if (voucherOrderMapper.selectById(order.getId()) != null) {
                return RocketMQLocalTransactionState.COMMIT;
            }
        } catch (Exception e) {
            databaseAvailable = false;
            log.error("回查秒杀订单DB状态失败: orderId={}", order.getId(), e);
        }

        try {
            String reservation = stringRedisTemplate.opsForValue().get(RESERVATION_KEY_PREFIX + order.getId());
            if (reservation != null && reservation.startsWith("RESERVED")) {
                return RocketMQLocalTransactionState.COMMIT;
            }
            if ("PROCESSING".equals(reservation)) {
                return RocketMQLocalTransactionState.UNKNOWN;
            }
            if (databaseAvailable) {
                stringRedisTemplate.opsForValue().set(
                        ORDER_STATUS_KEY + order.getId(), ORDER_STATUS_FAILED, ORDER_STATUS_TTL, TimeUnit.MINUTES);
                return RocketMQLocalTransactionState.ROLLBACK;
            }
        } catch (Exception e) {
            log.error("回查秒杀Redis预扣状态失败: orderId={}", order.getId(), e);
        }
        return RocketMQLocalTransactionState.UNKNOWN;
    }

    private MqBusinessMessage parse(Message message) {
        byte[] payload = (byte[]) message.getPayload();
        return JSONUtil.toBean(new String(payload, StandardCharsets.UTF_8), MqBusinessMessage.class);
    }
}
