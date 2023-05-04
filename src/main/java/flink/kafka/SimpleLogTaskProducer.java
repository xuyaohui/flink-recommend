package flink.kafka;

import flink.common.CommonConfig;
import lombok.extern.java.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 向kafka topic “logTask”发送测试消息
 */

@Log
public class SimpleLogTaskProducer {

    public static void main(String[] args) {
        String topicName = "logTask";
        Properties props = new Properties();

        props.put("bootstrap.servers", CommonConfig.FLINK_SERVER);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);//通过kafka进行消费来实现
        Long time = System.currentTimeMillis();
        String values="{\"name\":\"test1\",\"productId\":\"A2\",\"time\":"+time+",\"count\":\"4\"}";
        producer.send(new ProducerRecord<>(topicName,values));
        log.info("Message sent successfully");
        producer.close();

    }
}
