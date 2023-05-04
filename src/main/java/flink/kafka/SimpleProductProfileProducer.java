package flink.kafka;

import flink.common.CommonConfig;
import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Log
public class SimpleProductProfileProducer {

    public static void main(String[] args) {

        String topicName = "profile";
        Properties props = new Properties();

        props.put("bootstrap.servers", CommonConfig.FLINK_SERVER);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        Long time = System.currentTimeMillis();

        String values="{\"userId\":\"10\",\"productId\":\"2\",\"time\":"+time+",\"action\":\"2\"}";

        producer.send(new ProducerRecord<>(topicName,values));
        log.info("Message sent successfully");
        producer.close();
    }
}
