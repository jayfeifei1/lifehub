package com.hmdp.config;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hmdp.service.impl.VoucherOrderServiceImpl.STREAM_ORDERS_DLQ;

/**
 * 秒杀订单对账回补任务（方案A）
 *
 * 职责：周期性扫描死信队列 stream.orders.dlq，
 * 1. 若死信消息对应的 DB 订单已存在（期间重试/人工处理成功）-> 幂等，无需回补，直接删除死信消息；
 * 2. 若 DB 订单不存在 -> 说明 Redis 库存被预扣但订单永远无法创建，执行 restore.lua 回补库存并释放占位，再删除死信消息。
 *
 * 通过"死信队列 = 待回补清单"的机制，保证 Redis 库存与 DB 库存最终一致，避免库存被永久吞掉。
 */
@Slf4j
@Component
public class SeckillOrderReconcileTask {

    private static final DefaultRedisScript<Long> RESTORE_SCRIPT;

    static {
        RESTORE_SCRIPT = new DefaultRedisScript<>();
        RESTORE_SCRIPT.setLocation(new ClassPathResource("restore.lua"));
        RESTORE_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 每 60 秒执行一次对账
     */
    @Scheduled(fixedDelay = 60_000)
    public void reconcile() {
        //1.读取死信队列全部消息
        List<MapRecord<String, Object, Object>> records;
        try {
            records = stringRedisTemplate.opsForStream().range(STREAM_ORDERS_DLQ, Range.unbounded());
        } catch (Exception e) {
            log.error("读取死信队列失败", e);
            return;
        }
        if (records == null || records.isEmpty()) {
            return;
        }

        log.info("对账任务开始，死信消息数: {}", records.size());
        for (MapRecord<String, Object, Object> record : records) {
            String msgId = record.getId().getValue();
            try {
                Map<Object, Object> values = record.getValue();
                Long userId = Long.valueOf(String.valueOf(values.get("userId")));
                Long voucherId = Long.valueOf(String.valueOf(values.get("voucherId")));

                //2.查询 DB 订单是否已存在（幂等判断）
                Long count = voucherOrderService.query()
                        .eq("user_id", userId)
                        .eq("voucher_id", voucherId)
                        .count();
                if (count != null && count > 0) {
                    //3.订单已存在，无需回补
                    log.info("死信消息对应订单已存在，无需回补，删除死信: msgId={}, userId={}, voucherId={}",
                            msgId, userId, voucherId);
                } else {
                    //4.订单不存在：回补 Redis 库存 + 释放一人一单占位（原子执行）
                    stringRedisTemplate.execute(RESTORE_SCRIPT, Collections.emptyList(),
                            voucherId.toString(), userId.toString());
                    log.warn("已回补秒杀库存: msgId={}, userId={}, voucherId={}", msgId, userId, voucherId);
                }

                //5.删除死信消息（回补完成或无需回补，均移出队列）
                stringRedisTemplate.opsForStream().delete(STREAM_ORDERS_DLQ, msgId);
            } catch (Exception e) {
                //回补失败：保留死信消息，下轮对账重试
                log.error("对账回补失败，保留死信消息: msgId={}", msgId, e);
            }
        }
    }
}
