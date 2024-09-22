package com.riskmanager;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.riskmanager.condition.CustomMiddleCondition;
import com.riskmanager.condition.EndCondition;
import com.riskmanager.condition.MiddleCondition;
import com.riskmanager.condition.StartCondition;
import com.riskmanager.dynamic.JDBCPeriodicPatternProcessorDiscovererFactory;
import com.riskmanager.model.Transaction;
import com.riskmanager.model.TransactionDeserializationSchema;
import org.apache.flink.streaming.api.windowing.time.Time;


public class CepDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build Kafka source with new Source API based on FLIP-27
        KafkaSource<Transaction> kafkaSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("input")
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setValueOnlyDeserializer(new TransactionDeserializationSchema())
                        .build();
        // DataStream Source
        DataStreamSource<Transaction> source =
                env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.getTimestamp().getTime()),
                        "Kafka Source");

        env.setParallelism(1);

        // keyBy userId and productionId
        // Notes, only events with the same key will be processd to see if there is a match
        KeyedStream<Transaction, Tuple2<Long, Transaction>> keyedStream =
                source.keyBy(
                        new KeySelector<Transaction, Tuple2<Long, Transaction>>() {

                            @Override
                            public Tuple2<Long, Transaction> getKey(Transaction value) throws Exception {
                                return Tuple2.of(value.getUId(), value);
                            }
                        });

        // show how to print test pattern in json format
        Pattern<Transaction, Transaction> pattern =
        Pattern.<Transaction>begin("start")
            .where(new CustomMiddleCondition(new String[]{"amount > 10000", "A"}))
            .optional()
            .followedBy("middle")
            .where(new MiddleCondition())
            .timesOrMore(3)
            .notFollowedBy("end")
            .where(new EndCondition())
            .within(Time.minutes(10));
        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));

        // Dynamic CEP patterns
        SingleOutputStreamOperator<String> output =
                CEP.dynamicPatterns(
                        keyedStream,
                        new JDBCPeriodicPatternProcessorDiscovererFactory<>(
                                "jdbcUrl",
                                "com.mysql.cj.jdbc.Driver",
                                "tableName",
                                null,
                                3000L),
                        TimeBehaviour.ProcessingTime,
                        TypeInformation.of(new TypeHint<String>() {}));
        // Print output stream in taskmanager's stdout
//        output.print();
        output.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build());
        // Compile and submit the job
        env.execute("CEPDemo");
    }
}