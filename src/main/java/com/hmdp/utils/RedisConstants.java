package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    /** 登录态有效期：30 分钟，与 RefreshTokenInterceptor 的滑动续期保持一致（秒） */
    public static final Long LOGIN_USER_TTL = 1800L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final Long CACHE_USER_TTL = 30L;
    public static final String CACHE_USER_KEY = "cache:user:";
    /** 店铺类型列表缓存 TTL（分钟），类型变更最多延迟该时长生效 */
    public static final Long CACHE_SHOP_TYPE_TTL = 30L;

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;
    public static final String LOCK_CACHE_KEY = "lock:cache:";
    public static final Long LOCK_CACHE_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String CACHE_BLOG_HOT_KEY = "cache:blog:hot:";
    public static final Long CACHE_BLOG_HOT_TTL = 5L;
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    // 秒杀订单状态（异步下单结果通知窗口，TTL 过期后以 DB 为准）
    public static final String ORDER_STATUS_KEY = "order:status:";
    public static final Long ORDER_STATUS_TTL = 10L;
    public static final String ORDER_STATUS_CREATING = "CREATING";
    public static final String ORDER_STATUS_SUCCESS = "SUCCESS";
    public static final String ORDER_STATUS_FAILED = "FAILED";

    // 缓存删除补偿队列（删除缓存失败时入队，异步重试删除，保证最终一致）
    public static final String CACHE_DEL_QUEUE_KEY = "cache:del:queue";
    /** 补偿重试最大次数，超过后丢弃并告警（最终由缓存 TTL 兜底） */
    public static final int CACHE_DEL_MAX_RETRY = 5;
    /** 补偿重试计数 key 前缀 + 缓存key，用于记录每条待删 key 的重试次数 */
    public static final String CACHE_DEL_RETRY_KEY = "cache:del:retry:";
}
