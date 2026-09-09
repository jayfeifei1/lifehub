package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@Slf4j
@Component
@ConditionalOnProperty(name = "hmdp.cache.invalidation-mode", havingValue = "rocketmq")
@RocketMQMessageListener(
        topic = "${hmdp.rocketmq.topic.shop-update:lifehub-shop-update-tx}",
        consumerGroup = MqConstants.SHOP_CACHE_CONSUMER_GROUP,
        consumeThreadNumber = 2,
        consumeThreadMax = 8,
        maxReconsumeTimes = 16)
public class RocketMqShopCacheConsumer implements RocketMQListener<String> {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RocketMqBusinessProducer producer;

    @Override
    public void onMessage(String payload) {
        MqBusinessMessage message = JSONUtil.toBean(payload, MqBusinessMessage.class);
        Shop shop = message.getShop();
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        if (shop.getTypeId() != null && shop.getX() != null && shop.getY() != null) {
            String geoKey = SHOP_GEO_KEY + shop.getTypeId();
            stringRedisTemplate.opsForGeo().remove(geoKey, shop.getId().toString());
            stringRedisTemplate.opsForGeo().add(
                    geoKey, new Point(shop.getX(), shop.getY()), shop.getId().toString());
        }
        producer.sendDelayedShopCacheDelete(message);
        log.debug("RocketMQ首次删除店铺缓存完成: shopId={}", shop.getId());
    }
}
