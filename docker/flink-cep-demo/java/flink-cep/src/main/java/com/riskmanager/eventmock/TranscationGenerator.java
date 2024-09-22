package com.riskmanager.eventmock;


import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.riskmanager.model.Transaction;
import com.riskmanager.model.TransactionSerializationSchema;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * A generator which pushes {@link Transaction}s into a Kafka Topic configured via `--topic` and
 * `--bootstrap.servers`.
 *
 * <p> The generator creates the same number of {@link Transaction}s for all pages. The delay between
 * events is chosen such that processing time and event time roughly align. The generator always
 * creates the same sequence of events. </p>
 *
 */
public class TranscationGenerator {

    private static final List<Long> uIds = List.of(1001L, 1002L, 1003L);
    private static final List<Double> amounts = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);

    public static final long DELAY = 100;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String topic = params.get("topic", "input");

        Properties kafkaProps = createKafkaProperties(params);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps)) {
            TransactionIterator clickIterator = new TransactionIterator();

            while (true) {
                ProducerRecord<byte[], byte[]> record = new TransactionSerializationSchema(topic).serialize(
                        clickIterator.next(),
                        null);

                producer.send(record);
                Thread.sleep(DELAY);
            }
        }
    }

    private static Properties createKafkaProperties(final ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }

    static class TransactionIterator  {

        private final Map<Long, Long> nextTimestampPerKey;

        TransactionIterator() {
            nextTimestampPerKey = new HashMap<>();
        }

        Transaction next() {
            Long uId = nextUser();
            return new Transaction(uId, nextTimestamp(uId), nextT());
        }

        private Long nextUser() {
            return uIds.get(new Random().nextInt(uIds.size()));
        }

        private Date nextTimestamp(Long uid) {
            long nextTimestamp = nextTimestampPerKey.getOrDefault(uid, 0L);
            nextTimestampPerKey.put(uid, nextTimestamp + new Random().nextInt(100));
            return new Date(nextTimestamp);
        }

        private Double nextT() {
            return amounts.get(new Random().nextInt(amounts.size()));
        }
    }
}
