package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);


        //用逻辑过期来解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
        //cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class,this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }
    //创建一个线程池 大小为10
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //缓存击穿的解决方案  : 用逻辑过期
//    public Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id ;
//        //1.从redis中查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.判断是否存在
//        if (StrUtil.isBlank(shopJson)) {
//            //3.不存在 返回
//            return null;
//        }
//        //4.命中，需要先把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        JSONObject data = (JSONObject)redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //5.判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) { //过期时间在当前时间之后  则未过期
//            //5.1未过期 返回店铺信息
//            return shop;
//        }
//        //5.2已过期 需要缓存重建
//        //6.缓存重建
//        //6.1 获取互斥锁
//        String localKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(localKey);
//        //6.2 判断是否获取锁成功
//        if (isLock) {
//            //6.3 成功 ， 开启独立线程实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    //  Double Check（再查一次Redis）
//                    String json = stringRedisTemplate.opsForValue().get(key);
//                    RedisData redisDataNew = JSONUtil.toBean(json, RedisData.class);
//                    LocalDateTime expireTimeNew = redisDataNew.getExpireTime();
//                    if (expireTimeNew.isAfter(LocalDateTime.now())) {
//                        // 缓存已经被其他线程更新了，不需要重建
//                        return;
//                    }
//                    //重建缓存  仍然过期，才执行重建
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unlock(localKey);
//                }
//
//            });
//        }
//
//
//        //6.4 返回过期的商铺信息
//        return shop;
//    }


    //缓存穿透的解决方案
//    public Shop queryWithPassThrough(Long id){
//        String key = "cache:shop" + id ;
//        //1.从redis中查询商铺缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //3.存在，直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//        //判断命中的是否是空值 不等于null 即 等于""
//        if (shopJson != null) {
//            //返回错误信息
//            return null;
//        }
//        //4.不存在，根据id查询数据库
//        Shop shop = getById(id);
//        //5.数据库也不存在，返回错误
//        if (shop == null) {
//            //将空值写入redis
//            stringRedisTemplate.opsForValue().set(key,"",2L,TimeUnit.MINUTES);
//            return null;
//        }
//        //6.存在，写入redis
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
//        //7.返回
//        return shop;
//    }

    //缓存击穿的解决方案 ： 互斥锁
    public Shop queryWithMutex(Long id){
        String key = "cache:shop" + id ;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值 不等于null 即 等于""
        if (shopJson != null) {
            //返回错误信息
            return null;
        }

        //4.实现缓存重建
        //4.1 获取互斥锁
        String lockkey = "lock:shop:"+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockkey);
            //4.2 判断是否获取锁成功
            if(!isLock){
                //4.3 失败，休眠并且重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4 获取锁成功，再次检测redis 做doublecheck 如果存在则无需重建缓存
            //1.从redis中查询商铺缓存
            String shopJsonDouble = stringRedisTemplate.opsForValue().get(key);
            //2.判断是否存在
            if (StrUtil.isNotBlank(shopJsonDouble)) {
                //3.存在，直接返回
                shop = JSONUtil.toBean(shopJsonDouble, Shop.class);
                return shop;
            }
            //判断命中的是否是空值 不等于null 即 等于""
            if (shopJsonDouble != null) {
                //返回错误信息
                return null;
            }
            //依旧没有缓存 则根据id查询数据库
            shop = getById(id);
            //模拟重建的延迟
            Thread.sleep(200);
            //5.数据库也不存在，返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",2L,TimeUnit.MINUTES);
                return null;
            }
            //6.存在，写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7.释放互斥锁
            unlock(lockkey);
        }

        //8.返回
        return shop;
    }


    //获取锁的方法
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //flag != null && flag == true 的时候才返回 true
        return BooleanUtil.isTrue(flag);
    }
    //释放锁的方法
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //把shop作为数据 加个逻辑过期时间存入redis
    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete("cache:shop" + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断 是否需要根据坐标查询
        if(x == null || y == null){
            //不需要坐标查询  按照数据库查询
            Page<Shop> page = query().eq("type_id", typeId).
                    page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(page.getRecords());
        }

        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis  按照距离排序、分页  结果 shopId，distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        //4.解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if( list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        //4.1截取from到end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //4.2获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id",ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回
        return Result.ok(shops);
    }
}
