package com.hmdp.config;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

@Slf4j
@Component
public class SeckillStockSyncRunner implements CommandLineRunner {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void run(String... args) {
        List<SeckillVoucher> vouchers = seckillVoucherService.list();
        if (vouchers == null || vouchers.isEmpty()) {
            log.info("No seckill vouchers found, skip stock sync.");
            return;
        }

        int synced = 0;
        for (SeckillVoucher voucher : vouchers) {
            if (voucher.getVoucherId() == null || voucher.getStock() == null) {
                continue;
            }
            String key = SECKILL_STOCK_KEY + voucher.getVoucherId();
            stringRedisTemplate.opsForValue().set(key, voucher.getStock().toString());
            synced++;
        }
        log.info("Seckill stock synced to Redis: {} keys", synced);
    }
}
