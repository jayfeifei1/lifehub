package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@Slf4j
@Component
@ConditionalOnProperty(name = "hmdp.cache.invalidation-mode", havingValue = "rocketmq")
@RocketMQMessageListener(
        topic = "${hmdp.rocketmq.topic.shop-cache-delay:lifehub-shop-cache-delay}",
        consumerGroup = MqConstants.SHOP_CACHE_DELAY_CONSUMER_GROUP,
        consumeThreadNumber = 1,
        consumeThreadMax = 4,
        maxReconsumeTimes = 16)
public class RocketMqShopCacheDelayConsumer implements RocketMQListener<String> {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void onMessage(String payload) {
        MqBusinessMessage message = JSONUtil.toBean(payload, MqBusinessMessage.class);
        Long shopId = message.getShop().getId();
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
        log.debug("RocketMQ延迟二次删除店铺缓存完成: shopId={}", shopId);
    }
}
