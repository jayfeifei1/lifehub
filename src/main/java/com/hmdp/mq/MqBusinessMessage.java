package com.hmdp.mq;

import com.hmdp.entity.Shop;
import com.hmdp.entity.VoucherOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MqBusinessMessage implements Serializable {

    private String type;
    private String transactionId;
    private VoucherOrder order;
    private Shop shop;

    public static MqBusinessMessage seckill(VoucherOrder order) {
        return new MqBusinessMessage(
                MqConstants.TYPE_SECKILL_ORDER, String.valueOf(order.getId()), order, null);
    }

    public static MqBusinessMessage shopUpdate(String transactionId, Shop shop) {
        return new MqBusinessMessage(MqConstants.TYPE_SHOP_UPDATE, transactionId, null, shop);
    }
}
