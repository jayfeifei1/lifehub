package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @auther wty
 * @create 2025-07-02-15:18
 * @location HUBU
 * Description:
 */
@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    private void setWithRandomTtl(String key, Object value, Long time, TimeUnit unit){
        long ttlSeconds = unit.toSeconds(time) + ThreadLocalRandom.current().nextLong(60, 301);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), ttlSeconds, TimeUnit.SECONDS);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){

        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透的解决方案
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback
            ,Long time,TimeUnit unit){
        String key = keyPrefix + id ;
        //1.从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }

        //判断命中的是否是空值 不等于null 即 等于""
        if (json != null) {
            //返回错误信息
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.数据库也不存在，返回错误
        if (r  == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //6.存在，写入redis
        this.setWithRandomTtl(key,r,time,unit);
        //7.返回
        return r;
    }

    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                    Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            return null;
        }

        String lockKey = LOCK_CACHE_KEY + key;
        String lockValue = UUID.randomUUID().toString();
        boolean locked = false;
        try {
            while (!locked) {
                locked = tryLock(lockKey, lockValue);
                if (!locked) {
                    Thread.sleep(50);
                }
            }
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                return JSONUtil.toBean(json, type);
            }
            if (json != null) {
                return null;
            }

            R value = dbFallback.apply(id);
            if (value == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            setWithRandomTtl(key, value, time, unit);
            return value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("缓存重建被中断", e);
        } finally {
            if (locked) {
                unlock(lockKey, lockValue);
            }
        }
    }

    //缓存击穿的解决方案  : 用逻辑过期
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id,Class<R> type,Function<ID,R> dbFallBack
                                     ,Long time , TimeUnit unit){

        String key = keyPrefix + id ;
        //1.从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(json)) {
            //3.不存在 返回
            return null;
        }
        //4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject)redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) { //过期时间在当前时间之后  则未过期
            //5.1未过期 返回店铺信息
            return r;
        }
        //5.2已过期 需要缓存重建
        //6.缓存重建
        //6.1 获取互斥锁
        String localKey = LOCK_CACHE_KEY + key;
        String lockValue = UUID.randomUUID().toString();
        boolean isLock = tryLock(localKey, lockValue);
        //6.2 判断是否获取锁成功
        if (isLock) {
            //6.3 成功 ， 开启独立线程实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //  Double Check（再查一次Redis）
                    String jsonNew = stringRedisTemplate.opsForValue().get(key);
                    RedisData redisDataNew = JSONUtil.toBean(jsonNew, RedisData.class);
                    LocalDateTime expireTimeNew = redisDataNew.getExpireTime();
                    if (expireTimeNew.isAfter(LocalDateTime.now())) {
                        // 缓存已经被其他线程更新了，不需要重建
                        return;
                    }
                    //重建缓存  仍然过期，才执行重建
                    //查询数据库
                    R r1 = dbFallBack.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(localKey, lockValue);
                }
            });
        }
        //6.4 返回过期的商铺信息
        return r;
    }

    //获取锁的方法
    private boolean tryLock(String key, String value){
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, value, LOCK_CACHE_TTL, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    private void unlock(String key, String value){
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), value);
    }

}
