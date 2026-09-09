package com.hmdp.mq;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

@Service
public class ShopMqTransactionService {

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private ShopMapper shopMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateShopAndCommit(String transactionId, Shop shop) {
        if (shopMapper.updateById(shop) != 1) {
            throw new IllegalArgumentException("店铺不存在: " + shop.getId());
        }
        jdbcTemplate.update(
                "INSERT INTO tb_mq_transaction_log(transaction_id, business_type, business_id, status) VALUES (?, ?, ?, ?)",
                transactionId, MqConstants.TYPE_SHOP_UPDATE, String.valueOf(shop.getId()), MqConstants.TX_COMMITTED);
    }

    public String findStatus(String transactionId) {
        List<String> statuses = jdbcTemplate.queryForList(
                "SELECT status FROM tb_mq_transaction_log WHERE transaction_id = ?",
                String.class, transactionId);
        return statuses.isEmpty() ? null : statuses.get(0);
    }
}
