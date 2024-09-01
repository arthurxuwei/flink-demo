package com.riskmanager;


import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static com.riskmanager.Count.WINDOW_SIZE;

/**
 * A generator which pushes {@link ClickEvent}s into a Kafka Topic configured via `--topic` and
 * `--bootstrap.servers`.
 *
 * <p> The generator creates the same number of {@link ClickEvent}s for all pages. The delay between
 * events is chosen such that processing time and event time roughly align. The generator always
 * creates the same sequence of events. </p>
 *
 */
public class ClickEventGenerator {

    public static final int EVENTS_PER_WINDOW = 1000;

    private static final List<Long> uIds = List.of(1001L, 1002L, 1003L);
    private static final List<String> pages = Arrays.asList("/help", "/index", "/shop", "/jobs", "/about", "/news");

    //this calculation is only accurate as long as pages.size() * EVENTS_PER_WINDOW divides the
    //window size
    public static final long DELAY = WINDOW_SIZE.toMilliseconds() / pages.size() / EVENTS_PER_WINDOW;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        String topic = params.get("topic", "input");

        Properties kafkaProps = createKafkaProperties(params);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

        ClickIterator clickIterator = new ClickIterator();

        while (true) {

            ProducerRecord<byte[], byte[]> record = new ClickEventSerializationSchema(topic).serialize(
                    clickIterator.next(),
                    null);

            producer.send(record);

            Thread.sleep(DELAY);
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

    static class ClickIterator  {

        private final Map<Long, Long> nextTimestampPerKey;

        ClickIterator() {
            nextTimestampPerKey = new HashMap<>();
        }

        ClickEvent next() {
            Long uId = nextUser();
            String page = nextPage();
            return new ClickEvent(uId, nextTimestamp(uId), page);
        }

        private Long nextUser() {
            return uIds.get(new Random().nextInt(uIds.size()));
        }

        private Date nextTimestamp(Long uid) {
            long nextTimestamp = nextTimestampPerKey.getOrDefault(uid, 0L);
            nextTimestampPerKey.put(uid, nextTimestamp + WINDOW_SIZE.toMilliseconds() / EVENTS_PER_WINDOW);
            return new Date(nextTimestamp);
        }

        private String nextPage() {
            return pages.get(new Random().nextInt(pages.size()));
        }
    }
}
