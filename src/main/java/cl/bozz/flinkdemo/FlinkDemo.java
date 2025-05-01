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

public class FlinkDemo {
    public static void main(final String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka source
        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("input-topic")
                .setGroupId("flink-word-count-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create input stream from Kafka
        final DataStream<String> text = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // Process the input stream
        final DataStream<String> wordCounts = text
                .flatMap(new WordSplitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .sum(1)
                .map(tuple -> tuple.f0 + ":" + tuple.f1);

        // Configure Kafka sink
        final KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Write results to Kafka
        wordCounts.sinkTo(sink);

        // Execute the pipeline
        env.execute("Flink Word Count with Kafka");
    }

    // FlatMap function to split text into words and count occurrences
    public static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(final String value, final Collector<Tuple2<String, Integer>> out) {
            // Split the line into words
            final String[] words = value.toLowerCase().split("\\W+");

            // Emit each word with an initial count of 1
            for (final String word : words) {
                if (!word.isEmpty()) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
