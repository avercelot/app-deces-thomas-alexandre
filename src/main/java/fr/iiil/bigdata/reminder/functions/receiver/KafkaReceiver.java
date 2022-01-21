package fr.iiil.bigdata.reminder.functions.receiver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

@AllArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<String>> {
    private final List<String> topics;
    private final JavaStreamingContext javaStreamingContext;

    private final Map<String, Object> kafkaParams = new HashMap<String, Object>() {{
        put("bootstrap.servers", "localhost:9002");
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id", "spark-kafka-integ");
        put("auto.offset.reset", "earliest");
    }};
    @Override
    public JavaDStream<String> get() {
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );
        JavaDStream<String> javaDStream = directStream.map(ConsumerRecord::value);
        return javaDStream;
    }
}
