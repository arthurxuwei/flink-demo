package com.riskmanager;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;


public class AlertSerializationSchema implements SerializationSchema<Alert> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Alert event) {
        try {
            //if topic is null, default topic will be used
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + event, e);
        }
    }
}
