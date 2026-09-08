package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

/**
 * @auther wty
 * @create 2025-06-28-16:03
 * @location HUBU
 * Description:
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {
    //这个类加了Configuration 是由spring管理构建的，所有可以在这用resource注入一个redis管理对象来给我们new出来的LoginInterceptor初始化
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    //添加拦截器  设置哪些不需要被拦截
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //第二个拦截器 下方的是放行的路径 其他都拦截
        registry.addInterceptor(new LoginInterceptor())
                .excludePathPatterns(
                        // 店铺/类型/热点博客/登录 等浏览类接口放行
                        "/voucher/list/**",
                        "/shop/**",
                        "/shop-type/**",
                        "/blog/hot",
                        "/user/code",
                        "/user/login"
                ).order(1);
        //第一个拦截器  全部拦截 放到第二层拦截器再做细分  这一层主要做token时效刷新
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate)).addPathPatterns("/**").order(0);
    }
}
