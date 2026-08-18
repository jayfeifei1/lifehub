---
--- 对账回补脚本：恢复因订单永久失败而多扣的 Redis 库存，并释放一人一单占位
--- 由 SeckillOrderReconcileTask 在对账时执行（原子操作）
---
-- 1.参数列表
-- 1.1 优惠券id
local voucherId = ARGV[1]
-- 1.2 用户id
local userId = ARGV[2]

-- 2.数据key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 3.回补库存 +1（该用户下单失败，Redis 预扣的库存归还）
redis.call('incrby', stockKey, 1)
-- 4.释放一人一单占位，允许该用户重新参与秒杀
redis.call('srem', orderKey, userId)

return 0
