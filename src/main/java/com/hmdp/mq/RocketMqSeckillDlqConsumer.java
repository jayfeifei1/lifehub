package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hmdp.mq.MqConstants.RESERVATION_KEY_PREFIX;
import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
@ConditionalOnProperty(name = "hmdp.seckill.queue-mode", havingValue = "rocketmq")
@RocketMQMessageListener(
        topic = "${hmdp.rocketmq.topic.seckill-dlq:lifehub-seckill-order-dlq}",
        consumerGroup = MqConstants.SECKILL_DLQ_CONSUMER_GROUP,
        consumeThreadNumber = 1,
        consumeThreadMax = 4,
        maxReconsumeTimes = 16)
public class RocketMqSeckillDlqConsumer implements RocketMQListener<String> {

    private static final DefaultRedisScript<Long> RESTORE_MQ_SCRIPT;

    static {
        RESTORE_MQ_SCRIPT = new DefaultRedisScript<>();
        RESTORE_MQ_SCRIPT.setLocation(new ClassPathResource("restore_mq.lua"));
        RESTORE_MQ_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String payload) {
        MqBusinessMessage message = JSONUtil.toBean(payload, MqBusinessMessage.class);
        VoucherOrder order = message.getOrder();
        Long count = voucherOrderService.query()
                .eq("user_id", order.getUserId())
                .eq("voucher_id", order.getVoucherId())
                .count();
        if (count != null && count > 0) {
            stringRedisTemplate.delete(RESERVATION_KEY_PREFIX + order.getId());
            stringRedisTemplate.opsForValue().set(
                    ORDER_STATUS_KEY + order.getId(), ORDER_STATUS_SUCCESS, ORDER_STATUS_TTL, TimeUnit.MINUTES);
            log.info("RocketMQ死信订单已存在，无需回补: orderId={}", order.getId());
            return;
        }

        Long restored = stringRedisTemplate.execute(
                RESTORE_MQ_SCRIPT,
                Collections.emptyList(),
                order.getVoucherId().toString(),
                order.getUserId().toString(),
                order.getId().toString());
        if (restored == null) {
            throw new IllegalStateException("Redis库存回补未返回结果: " + order.getId());
        }
        stringRedisTemplate.opsForValue().set(
                ORDER_STATUS_KEY + order.getId(), ORDER_STATUS_FAILED, ORDER_STATUS_TTL, TimeUnit.MINUTES);
        log.warn("RocketMQ死信订单对账完成: orderId={}, restored={}", order.getId(), restored == 1);
    }
}
