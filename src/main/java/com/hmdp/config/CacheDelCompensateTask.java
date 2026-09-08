package com.hmdp.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * 缓存删除补偿任务（消息队列方案）
 *
 * 背景：Cache Aside 中"更新 DB 后删除缓存"如果删除失败（Redis 抖动/不可用），
 * 旧缓存会残留导致不一致。生产级做法是把待删 key 投递到消息队列，后台消费者
 * 持续重试删除，直到成功。
 *
 * 本项目未引入 MQ，使用 Redis List 作为轻量队列（cache:del:queue）模拟 MQ 语义：
 * - 写入方（ShopServiceImpl.updateShop）：删除失败 → key 入队（LPUSH）
 * - 消费方（本任务）：定时出队（RPOP）重试删除，成功即完成；
 *   失败则重试计数 +1，未超限重新入队（下次再试），超限丢弃并告警（由缓存 TTL 最终兜底）
 *
 * 配合延迟双删：补偿任务解决"删除失败"，延迟双删解决"删除后并发回填旧值"，两者互补。
 */
@Slf4j
@Component
public class CacheDelCompensateTask {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 每 5 秒扫描一次补偿队列，重试删除失败的缓存 key
     */
    @Scheduled(fixedDelay = 5_000)
    public void compensate() {
        int processed = 0;
        List<String> retryKeys = new ArrayList<>();
        while (true) {
            //1.从队列尾部取一个待删 key（LPUSH/RPOP = FIFO）
            String cacheKey = stringRedisTemplate.opsForList().rightPop(CACHE_DEL_QUEUE_KEY);
            if (cacheKey == null) {
                break;   // 队列空，结束本轮
            }
            processed++;
            try {
                //2.重试删除缓存
                stringRedisTemplate.delete(cacheKey);
                //3.删除成功：清除重试计数
                stringRedisTemplate.delete(CACHE_DEL_RETRY_KEY + cacheKey);
                log.info("补偿删除缓存成功: {}", cacheKey);
            } catch (Exception e) {
                //4.删除仍失败：累计重试次数
                Long retries = stringRedisTemplate.opsForValue().increment(CACHE_DEL_RETRY_KEY + cacheKey);
                //计数 key 带 1 小时 TTL，避免长期占用
                stringRedisTemplate.expire(CACHE_DEL_RETRY_KEY + cacheKey, 1, TimeUnit.HOURS);
                if (retries != null && retries >= CACHE_DEL_MAX_RETRY) {
                    //5.超过最大重试次数：丢弃并告警（缓存自带 TTL，最终会过期收敛）
                    log.error("补偿删除缓存超过最大重试次数({})，丢弃，依赖缓存TTL兜底: {}",
                            CACHE_DEL_MAX_RETRY, cacheKey);
                    stringRedisTemplate.delete(CACHE_DEL_RETRY_KEY + cacheKey);
                } else {
                    //6.未超限：本轮结束后再重新入队，下一次定时任务再试，避免同一轮立即耗尽重试次数
                    retryKeys.add(cacheKey);
                    log.warn("补偿删除缓存失败，第{}次重试将在下一轮执行: {}", retries, cacheKey);
                }
            }
        }
        for (String cacheKey : retryKeys) {
            try {
                stringRedisTemplate.opsForList().leftPush(CACHE_DEL_QUEUE_KEY, cacheKey);
            } catch (Exception e) {
                log.error("补偿缓存重新入队失败，依赖缓存 TTL 兜底: {}", cacheKey, e);
            }
        }
        if (processed > 0) {
            log.debug("缓存删除补偿任务处理完成: {} 条", processed);
        }
    }
}
