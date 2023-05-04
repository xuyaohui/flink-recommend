package flink.function;


import com.alibaba.fastjson.JSONObject;
import flink.domin.LogEntity;
import org.apache.flink.api.common.functions.MapFunction;

public class TopProductMapFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {
        return JSONObject.parseObject(s, LogEntity.class);
    }
}
