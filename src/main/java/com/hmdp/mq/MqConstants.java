package com.hmdp.mq;

public final class MqConstants {

    public static final String TYPE_SECKILL_ORDER = "SECKILL_ORDER";
    public static final String TYPE_SHOP_UPDATE = "SHOP_UPDATE";

    public static final String DEFAULT_SECKILL_TOPIC = "lifehub-seckill-order-tx";
    public static final String DEFAULT_SECKILL_DLQ_TOPIC = "lifehub-seckill-order-dlq";
    public static final String DEFAULT_SHOP_UPDATE_TOPIC = "lifehub-shop-update-tx";
    public static final String DEFAULT_SHOP_CACHE_DELAY_TOPIC = "lifehub-shop-cache-delay";

    public static final String SECKILL_CONSUMER_GROUP = "lifehub-seckill-order-consumer";
    public static final String SECKILL_DLQ_CONSUMER_GROUP = "lifehub-seckill-dlq-consumer";
    public static final String SHOP_CACHE_CONSUMER_GROUP = "lifehub-shop-cache-consumer";
    public static final String SHOP_CACHE_DELAY_CONSUMER_GROUP = "lifehub-shop-cache-delay-consumer";

    public static final String TX_COMMITTED = "COMMITTED";

    public static final String RESERVATION_KEY_PREFIX = "seckill:reservation:";

    private MqConstants() {
    }
}
