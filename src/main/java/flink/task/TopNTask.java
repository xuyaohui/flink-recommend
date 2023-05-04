package flink.task;


import flink.domin.LogEntity;
import flink.domin.TopProductEntity;
import flink.function.TopProductMapFunction;
import flink.top.CountAgg;
import flink.top.TopNHotItems;
import flink.top.TopNRedisSink;
import flink.top.WindowResultFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.List;
import java.util.Properties;

/**
 * 热门商品 -> redis
 */
public class TopNTask {

    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {
        String redisHost = "192.168.56.10";
        String topicName = "topN";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 导入 flink-redis的使用
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(redisHost).build();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.10:9092");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>(topicName, new SimpleStringSchema(), properties));

        DataStream<TopProductEntity> topProduct = dataStream.map(new TopProductMapFunction()).
                // 抽取时间戳做 watermark 以 秒 为单位
                assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEntity>() {
                    @Override
                    public long extractAscendingTimestamp(LogEntity logEntity) {
                        return logEntity.getTime();
                    }
                })
                // 按照productId 按滑动窗口
                .keyBy("productId").window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new CountAgg(),new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new TopNHotItems(topSize)).flatMap(new FlatMapFunction<List<String>, TopProductEntity>() {
                    @Override
                    public void flatMap(List<String> strings, Collector<TopProductEntity> collector) throws Exception {
                        System.out.println("-------------Top N Product------------");
                        for (int i = 0; i < strings.size(); i++) {
                            TopProductEntity top = new TopProductEntity();
                            top.setRankName(String.valueOf(i));
                            top.setProductId(strings.get(i));
                            // 输出排名结果
                            System.out.println(top);
                            collector.collect(top);
                        }

                    }
                });


        topProduct.addSink(new RedisSink<>(conf,new TopNRedisSink()));
        env.execute("Top N ");
    }
}
