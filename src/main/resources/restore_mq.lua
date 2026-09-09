-- RocketMQ死信回补。reservation标记是幂等开关，重复消费不会重复增加库存。
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local reservationKey = 'seckill:reservation:' .. orderId
local statusKey = 'order:status:' .. orderId

local reservation = redis.call('get', reservationKey)
if not reservation or string.sub(reservation, 1, 8) ~= 'RESERVED' then
    return 0
end

redis.call('incrby', stockKey, 1)
redis.call('srem', orderKey, userId)
redis.call('del', reservationKey)
redis.call('set', statusKey, 'FAILED', 'EX', 600)
return 1
