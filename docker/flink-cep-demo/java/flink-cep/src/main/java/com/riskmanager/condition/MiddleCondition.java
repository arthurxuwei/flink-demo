package com.riskmanager.condition;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import com.riskmanager.model.Transaction;

public class MiddleCondition extends SimpleCondition<Transaction> {

    @Override
    public boolean filter(Transaction value) throws Exception {
        return value.getUId() == 1001L;
    }
}
