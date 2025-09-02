package com.hmdp.utils;

/**
 * @auther wty
 * @create 2025-07-05-19:14
 * @location HUBU
 * Description:
 */
public interface ILock {
    //获取锁
    boolean tryLock(long timeoutSec);


    //释放锁
    void unlock();


}
