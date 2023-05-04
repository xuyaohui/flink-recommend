package flink.kafka;

import lombok.extern.java.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

@Log
public class SimpleConsumer {

    public static void main(String[] args) {

        String topicName = "Hello-Kafka";
        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.56.10:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");//注意是StringDeserializer
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Subscribed to topic：" + topicName);
        while (true) {
            ConsumerRecords<Object, Object> records = consumer.poll(1000);
            for (ConsumerRecord<Object, Object> record : records) {
                System.out.printf("xuyaohui: offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            }
        }
    }

}
