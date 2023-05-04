package flink.kafka;

import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Properties;

@Log
public class SimpleProducer {

    public static void main(String[] args) {
        String topicName = "Hello-Kafka";
        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.56.10:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        Long time = System.currentTimeMillis();
        
        String values="{\"name\":\"test2\",\"productId\":\"A4\",\"time\":"+time+",\"count\":\"2\"}";
        //String values = "{\"name\":\"qwer\",\"count\":\"11111\"}";
        producer.send(new ProducerRecord<>(topicName,values));
        log.info("Message sent successfully");
        producer.close();
    }
}
