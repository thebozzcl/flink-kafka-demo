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

public class FlinkDemo {
    public static void main(final String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("input-topic")
                .setGroupId("flink-word-count-group")
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
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .sum(1)
                .map(tuple -> tuple.f0 + ":" + tuple.f1);

        final KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output-topic")
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
