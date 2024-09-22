package com.riskmanager.condition;
import org.apache.flink.cep.dynamic.condition.AviatorCondition;

import com.riskmanager.model.Transaction;

public class StartCondition extends AviatorCondition<Transaction> {

    public StartCondition(String expression) {
        super(expression);
    }
}