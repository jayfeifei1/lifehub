package com.hmdp.dto;

/**
 * 订单创建/处理结果（三态）
 */
public enum OrderHandleResult {
    /** 处理成功（已建单或幂等命中），消息可 ACK */
    SUCCESS,
    /** 可重试失败（分布式锁竞争、DB 临时异常），消息保留在 pending-list 稍后重试 */
    RETRYABLE,
    /** 永久失败（DB 库存不足等），消息转死信队列等待对账回补 */
    FATAL
}
