package cl.bozz.flinkdemo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.time.Duration;
import java.util.Arrays;

public class WindowedKafkaWordCountDemo {
    private static final Duration WINDOW_DURATION = Duration.ofMinutes(1);
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String KAFKA_INPUT_TOPIC = "input-topic";
    private static final String KAFKA_OUTPUT_TOPIC = "output-topic";
    private static final String KAFKA_GROUP_ID = "flink-demo-group";

    public static void main(final String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_INPUT_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final DataStream<String> text = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        final DataStream<String> wordCounts = text
                .flatMap(new WordSplitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(WINDOW_DURATION))
                .sum(1)
                .map(tuple -> tuple.f0 + ":" + tuple.f1);

        final KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(KAFKA_OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        wordCounts.sinkTo(sink);

        env.execute("Flink word-count demo with Kafka I/O");
    }

    public static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(final String value, final Collector<Tuple2<String, Integer>> out) {
            Arrays.stream(value.toLowerCase().split("\\W+"))
                    .filter(word -> !word.isEmpty())
                    .map(word -> new Tuple2<>(word, 1))
                    .forEach(out::collect);
        }
    }
}
