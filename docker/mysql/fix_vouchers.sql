-- 修正已有秒杀券的过期时间（原来已过期导致前端 isEnd 隐藏）
UPDATE tb_seckill_voucher
SET begin_time = '2026-08-18 00:00:00', end_time = '2027-01-01 23:59:59'
WHERE voucher_id = 11;

-- 给所有店铺补普通券（已有券的店铺跳过）
INSERT INTO tb_voucher (shop_id, title, sub_title, rules, pay_value, actual_value, type, status)
SELECT s.id, '50元代金券', '满100元可用', '全场通用，不与其它优惠同享，有效期至2027-01-01', 4750, 5000, 0, 1
FROM tb_shop s
WHERE NOT EXISTS (SELECT 1 FROM tb_voucher v WHERE v.shop_id = s.id AND v.type = 0);

-- 给前 6 家店铺补秒杀券
INSERT INTO tb_voucher (shop_id, title, sub_title, rules, pay_value, actual_value, type, status)
SELECT s.id, '1元秒杀100元代金券', '限时抢购', '每人限购1张，有效期至2027-01-01', 100, 10000, 1, 1
FROM tb_shop s
WHERE s.id <= 6
  AND NOT EXISTS (SELECT 1 FROM tb_voucher v WHERE v.shop_id = s.id AND v.type = 1);

-- 为新秒杀券补充秒杀信息（库存 50，未过期）
INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)
SELECT v.id, 50, '2026-08-18 00:00:00', '2027-01-01 23:59:59'
FROM tb_voucher v
WHERE v.type = 1
  AND NOT EXISTS (SELECT 1 FROM tb_seckill_voucher sv WHERE sv.voucher_id = v.id);
