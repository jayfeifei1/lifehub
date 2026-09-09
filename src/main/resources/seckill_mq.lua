-- RocketMQ模式秒杀资格预扣。
-- Half消息已由Broker持久化后才执行本脚本；预扣成功后事务消息才会Commit。
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local reservationKey = 'seckill:reservation:' .. orderId
local statusKey = 'order:status:' .. orderId

local reservation = redis.call('get', reservationKey)
if reservation and string.sub(reservation, 1, 8) == 'RESERVED' then
    return 0
end

local stock = tonumber(redis.call('get', stockKey))
if stock == nil or stock <= 0 then
    redis.call('del', reservationKey)
    return 1
end

if redis.call('sismember', orderKey, userId) == 1 then
    redis.call('del', reservationKey)
    return 2
end

redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
redis.call('set', reservationKey, 'RESERVED:' .. userId .. ':' .. voucherId)
redis.call('set', statusKey, 'CREATING', 'EX', 600)
return 0
