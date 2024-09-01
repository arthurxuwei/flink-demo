package com.riskmanager;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Count {
    public static final String CHECKPOINTING_OPTION = "checkpointing";
    public static final String OPERATOR_CHAINING_OPTION = "chaining";

    public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureEnvironment(params, env);


        String inputTopic = params.get("input-topic", "input");
        String outputTopic = params.get("output-topic", "output");
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count");

        KafkaSource<ClickEvent> source = KafkaSource.<ClickEvent>builder()
                .setTopics(inputTopic)
                .setValueOnlyDeserializer(new ClickEventDeserializationSchema())
                .setProperties(kafkaProps)
                .build();

        WatermarkStrategy<ClickEvent> watermarkStrategy = WatermarkStrategy
                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofMillis(200))
                .withIdleness(Duration.ofSeconds(5))
                .withTimestampAssigner((clickEvent, l) -> clickEvent.getTimestamp().getTime());

        DataStream<ClickEvent> clicks = env.fromSource(source, watermarkStrategy, "ClickEvent Source");

        KeyedStream<ClickEvent, Long> clicksByUser = clicks.keyBy((KeySelector<ClickEvent, Long>) ClickEvent::getUId);


        Pattern<ClickEvent, ?> pattern = Pattern.<ClickEvent>begin("start")
                .next("middle")
                .where(SimpleCondition.of(value -> value.getPage().equals("/index")))
                .followedBy("end")
                .where(SimpleCondition.of(value -> value.getPage().equals("/shop")))
                .within(Duration.ofSeconds(5));


        PatternStream<ClickEvent> patternStream = CEP.pattern(clicksByUser, pattern);


        DataStream<Alert> alerts = patternStream.select((PatternSelectFunction<ClickEvent, Alert>) pattern1 -> new Alert(pattern1.toString()));

        alerts.sinkTo(
                KafkaSink.<Alert>builder()
                        .setBootstrapServers(kafkaProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                        .setKafkaProducerConfig(kafkaProps)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(outputTopic)
                                        .setValueSerializationSchema(new AlertSerializationSchema())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build())
        .name("Alert Sink");

        env.execute("Click Event Count");
    }

    private static void configureEnvironment(
            final ParameterTool params,
            final StreamExecutionEnvironment env) {

        boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
        boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

        if (checkpointingEnabled) {
            env.enableCheckpointing(1000);
        }

        if(!enableChaining){
            //disabling Operator chaining to make it easier to follow the Job in the WebUI
            env.disableOperatorChaining();
        }
    }

}
