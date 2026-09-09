package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.UUID;

@Slf4j
@Component
public class RocketMqBusinessProducer {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Value("${hmdp.rocketmq.topic.seckill-order:" + MqConstants.DEFAULT_SECKILL_TOPIC + "}")
    private String seckillTopic;

    @Value("${hmdp.rocketmq.topic.seckill-dlq:" + MqConstants.DEFAULT_SECKILL_DLQ_TOPIC + "}")
    private String seckillDlqTopic;

    @Value("${hmdp.rocketmq.topic.shop-update:" + MqConstants.DEFAULT_SHOP_UPDATE_TOPIC + "}")
    private String shopUpdateTopic;

    @Value("${hmdp.rocketmq.topic.shop-cache-delay:" + MqConstants.DEFAULT_SHOP_CACHE_DELAY_TOPIC + "}")
    private String shopCacheDelayTopic;

    public Result sendSeckillOrder(Long voucherId, Long userId, Long orderId) {
        VoucherOrder order = new VoucherOrder()
                .setId(orderId)
                .setVoucherId(voucherId)
                .setUserId(userId);
        MqTransactionContext context = new MqTransactionContext(MqBusinessMessage.seckill(order));
        try {
            rocketMQTemplate.sendMessageInTransaction(
                    seckillTopic, buildMessage(context.getMessage(), orderId.toString()), context);
        } catch (Exception e) {
            if (!context.isLocalTransactionStarted()) {
                log.error("RocketMQ Half消息发送失败，未执行Redis预扣: orderId={}", orderId, e);
                return Result.fail("系统繁忙，请稍后重试");
            }
            log.warn("秒杀本地事务结果待Broker回查: orderId={}", orderId, e);
        }

        Integer result = context.getBusinessResult();
        if (result != null && result != 0) {
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        return Result.ok(orderId);
    }

    public Result sendShopUpdate(Shop shop) {
        String transactionId = UUID.randomUUID().toString().replace("-", "");
        MqTransactionContext context = new MqTransactionContext(
                MqBusinessMessage.shopUpdate(transactionId, shop));
        try {
            rocketMQTemplate.sendMessageInTransaction(
                    shopUpdateTopic, buildMessage(context.getMessage(), shop.getId().toString()), context);
        } catch (Exception e) {
            if (context.isLocalTransactionCommitted()) {
                log.warn("店铺已提交，RocketMQ二次确认异常，等待Broker回查: transactionId={}", transactionId, e);
                return Result.ok();
            }
            log.error("店铺更新事务消息发送失败: transactionId={}", transactionId, e);
            return Result.fail(context.getErrorMessage() == null ? "店铺更新失败" : context.getErrorMessage());
        }

        if (!context.isLocalTransactionCommitted()) {
            return Result.fail(context.getErrorMessage() == null ? "店铺更新失败" : context.getErrorMessage());
        }
        return Result.ok();
    }

    public void sendSeckillDlq(MqBusinessMessage message) {
        rocketMQTemplate.syncSend(
                seckillDlqTopic, buildMessage(message, message.getOrder().getId().toString()));
    }

    public void sendDelayedShopCacheDelete(MqBusinessMessage message) {
        rocketMQTemplate.syncSendDelayTimeSeconds(
                shopCacheDelayTopic, buildMessage(message, message.getShop().getId().toString()), 1);
    }

    private Message<String> buildMessage(MqBusinessMessage message, String key) {
        return MessageBuilder.withPayload(JSONUtil.toJsonStr(message))
                .setHeader(RocketMQHeaders.KEYS, key)
                .build();
    }
}
