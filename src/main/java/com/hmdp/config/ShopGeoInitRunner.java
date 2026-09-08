package com.hmdp.config;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

/**
 * 商铺 GEO 数据初始化：
 * 启动时将 tb_shop 中所有店铺按类型分组写入 Redis GEO（shop:geo:{typeId}），
 * 供附近商铺按距离排序查询（queryShopByType）使用。
 * 新增/更新店铺时由 ShopServiceImpl.addShop/updateShop 同步维护。
 */
@Slf4j
@Component
public class ShopGeoInitRunner implements CommandLineRunner {

    @Resource
    private IShopService shopService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void run(String... args) {
        try {
            List<Shop> shops = shopService.list();
            if (shops == null || shops.isEmpty()) {
                log.info("No shops found, skip GEO init.");
                return;
            }
            //按类型分组（typeId 为空或坐标缺失的店铺跳过）
            Map<Long, List<Shop>> byType = shops.stream()
                    .filter(s -> s.getTypeId() != null && s.getX() != null && s.getY() != null)
                    .collect(Collectors.groupingBy(Shop::getTypeId));
            int total = 0;
            for (Map.Entry<Long, List<Shop>> entry : byType.entrySet()) {
                String key = SHOP_GEO_KEY + entry.getKey();
                for (Shop shop : entry.getValue()) {
                    stringRedisTemplate.opsForGeo().add(
                            key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                    total++;
                }
            }
            log.info("Shop GEO initialized: {} types, {} shops", byType.size(), total);
        } catch (Exception e) {
            log.error("Shop GEO init failed", e);
        }
    }
}
