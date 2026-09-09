package com.hmdp.mq;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MqTransactionContext {

    private final MqBusinessMessage message;
    private boolean localTransactionStarted;
    private boolean localTransactionCommitted;
    private Integer businessResult;
    private String errorMessage;

    public MqTransactionContext(MqBusinessMessage message) {
        this.message = message;
    }
}
