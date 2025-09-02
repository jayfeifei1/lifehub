package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryList() {
        String key = "cache:shopList";
        //1.从redis中查询商铺列表的缓存
        String shopListJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopListJson)) {
            //3.存在，直接返回
            List<ShopType> typeList = JSONUtil.toList(shopListJson, ShopType.class);
            return Result.ok(typeList);
        }
        //4.不存在，则查询数据库
               List<ShopType> typeList = query().orderByAsc("sort").list();
        //5.不存在，返回错误
        if (typeList == null || typeList.isEmpty()) {
            return Result.fail("店铺类型列表不存在！");
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(typeList));
        //7.返回
        return Result.ok(typeList);

    }
}
