package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.OrderHandleResult;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

import static com.hmdp.mq.MqConstants.RESERVATION_KEY_PREFIX;

@Slf4j
@Component
@ConditionalOnProperty(name = "hmdp.seckill.queue-mode", havingValue = "rocketmq")
@RocketMQMessageListener(
        topic = "${hmdp.rocketmq.topic.seckill-order:lifehub-seckill-order-tx}",
        consumerGroup = MqConstants.SECKILL_CONSUMER_GROUP,
        consumeThreadNumber = 4,
        consumeThreadMax = 16,
        maxReconsumeTimes = 3)
public class RocketMqSeckillOrderConsumer implements RocketMQListener<MessageExt> {

    private static final int MAX_RECONSUME_TIMES = 3;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RocketMqBusinessProducer producer;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(MessageExt rawMessage) {
        MqBusinessMessage message = JSONUtil.toBean(
                new String(rawMessage.getBody(), StandardCharsets.UTF_8), MqBusinessMessage.class);
        VoucherOrder order = message.getOrder();
        OrderHandleResult result = voucherOrderService.handleVoucherOrder(order);
        if (result == OrderHandleResult.SUCCESS) {
            stringRedisTemplate.delete(RESERVATION_KEY_PREFIX + order.getId());
            return;
        }
        if (result == OrderHandleResult.FATAL) {
            producer.sendSeckillDlq(message);
            log.error("RocketMQ秒杀订单永久失败，已进入业务死信队列: orderId={}", order.getId());
            return;
        }
        if (rawMessage.getReconsumeTimes() >= MAX_RECONSUME_TIMES) {
            producer.sendSeckillDlq(message);
            log.error("RocketMQ秒杀订单重试超限，已进入业务死信队列: orderId={}", order.getId());
            return;
        }
        throw new IllegalStateException("秒杀订单暂时处理失败，交由RocketMQ重试: " + order.getId());
    }
}
