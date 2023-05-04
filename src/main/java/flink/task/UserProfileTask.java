package flink.task;

import flink.common.CommonConfig;
import flink.function.LogMapFunction;
import flink.function.UserPortraitMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 用户画像 -> mongodb
 *
 *
 * 2023-04-20 新增产品-用户表，用于计算产品之前相似度
 */
public class UserProfileTask {

    public static void main(String[] args) throws Exception {

        String topicName = "userProfile";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConfig.FLINK_SERVER);

        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>(topicName, new org.apache.flink.streaming.util.serialization.SimpleStringSchema(), properties));
        dataStream.map(new UserPortraitMapFunction());
        env.execute("User profile");
    }
}
