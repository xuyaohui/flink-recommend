package flink.task;


import flink.function.ProductPortraitMapFunction;
import flink.utils.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;


public class ProductProtaritTask {

    public static void main(String[] args) throws Exception {

        String topicName = "profile";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties(topicName);
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), properties));
        dataStream.map(new ProductPortraitMapFunction());
        env.execute("Product Portrait");

    }
}