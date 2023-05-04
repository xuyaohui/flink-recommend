package flink.function;

import com.alibaba.fastjson.JSON;
import flink.utils.MongoDBUtil;
import lombok.extern.java.Log;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * @author yaohui
 */

@Log
public class LogMapFunction implements MapFunction<String, String> {

    private final String CollectionName = "logTask";

    @Override
    public String map(String s) throws Exception {
        log.info("received message: " + s);
        HashMap hashMap = JSON.parseObject(s, HashMap.class);
        MongoDBUtil.getInstance().addOne(hashMap, CollectionName);
        return s;
    }
}
