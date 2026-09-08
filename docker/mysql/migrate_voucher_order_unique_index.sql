-- 运行前先确认不存在重复的 (user_id, voucher_id) 订单。
ALTER TABLE tb_voucher_order
    ADD UNIQUE KEY uk_voucher_order_user_voucher (user_id, voucher_id);
