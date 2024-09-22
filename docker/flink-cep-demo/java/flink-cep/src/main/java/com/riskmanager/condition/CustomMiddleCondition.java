package com.riskmanager.condition;


import org.apache.flink.cep.dynamic.condition.CustomArgsCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import com.riskmanager.model.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** CustomMiddleCondition. */
public class CustomMiddleCondition extends CustomArgsCondition<Transaction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CustomMiddleCondition.class);

    private final transient Expression compiledExpression;
    // Events that does not belong to target group will not be considered
    private final Set<String> targetGroup;

    public CustomMiddleCondition(String[] args) {
        this(args, CustomMiddleCondition.class.getCanonicalName());
    }

    public CustomMiddleCondition(String[] args, String className) {
        super(args, className);
        checkExpression(args[0]);
        compiledExpression = AviatorEvaluator.compile(args[0], false);
        targetGroup = new HashSet<>();
        targetGroup.addAll(Arrays.asList(args).subList(1, args.length));
    }

    private void checkExpression(String expression) {
        try {
            AviatorEvaluator.validate(expression);
        } catch (ExpressionSyntaxErrorException | CompileExpressionErrorException e) {
            throw new IllegalArgumentException(
                    "The expression of AviatorCondition is invalid: " + e.getMessage());
        }
    }

    @Override
    public boolean filter(Transaction event) throws Exception {
        if (compiledExpression == null) {
            LOG.warn("The Aviator Expression is Null. Will return false as fiter result.");
            return false;
        }
        List<String> variableNames = compiledExpression.getVariableNames();
        if (variableNames.isEmpty()) {
            return true;
        }

        Map<String, Object> variables = new HashMap<>();
        for (String variableName : variableNames) {
            Object variableValue = getVariableValue(event, variableName);
            if (variableName.equals("eventArgs")) {
                ObjectMapper mapper = new ObjectMapper();
                JsonFactory factory = mapper.getFactory();
                JsonParser parser = factory.createParser((String) variableValue);
                JsonNode actualObj = mapper.readTree(parser);
                if (actualObj.get("group") != null) {
                    // Skip events whose group does not fall in the target groups
                    if (!targetGroup.contains(actualObj.get("group").asText())) {
                        return false;
                    }
                }
                variables.put(
                        variableName,
                        mapper.convertValue(
                                actualObj, new TypeReference<Map<String, Object>>() {}));
            } else {
                if (!Objects.isNull(variableValue)) {
                    variables.put(variableName, variableValue);
                }
            }
        }
        return (Boolean) compiledExpression.execute(variables);
    }

    public Object getVariableValue(Transaction propertyBean, String variableName)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = propertyBean.getClass().getDeclaredField(variableName);
        field.setAccessible(true);
        return field.get(propertyBean);
    }
}
