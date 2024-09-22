package com.riskmanager.model;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class TransactionSerializationSchema implements KafkaSerializationSchema<Transaction> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private String topic;

    public TransactionSerializationSchema(){
    }

    public TransactionSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            final Transaction message, @Nullable final Long timestamp) {
        try {
            //if topic is null, default topic will be used
            return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(message));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + message, e);
        }
    }
}
