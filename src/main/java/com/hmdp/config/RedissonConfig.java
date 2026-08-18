package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @auther wty
 * @create 2025-07-06-17:30
 * @location HUBU
 * Description:
 */
@Configuration
public class RedissonConfig {

    private final RedisProperties redisProperties;

    public RedissonConfig(RedisProperties redisProperties) {
        this.redisProperties = redisProperties;
    }

    @Bean
    public RedissonClient redissonClient(){
        // 复用 spring.redis 配置，避免 Redisson 与 StringRedisTemplate 连接到不同地址
        Config config = new Config();
        String address = "redis://" + redisProperties.getHost() + ":" + redisProperties.getPort();
        String password = redisProperties.getPassword();
        if (password == null || password.trim().isEmpty()) {
            config.useSingleServer().setAddress(address);
        } else {
            config.useSingleServer().setAddress(address).setPassword(password);
        }
        //创建redissonClient对象
        return Redisson.create(config);
    }
}
