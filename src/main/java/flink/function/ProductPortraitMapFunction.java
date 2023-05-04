package flink.function;


import com.alibaba.fastjson.JSON;
import flink.domin.LogEntity;
import flink.utils.MongoDBUtil;
import flink.utils.MysqlClient;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.ResultSet;
import java.util.HashMap;


public class ProductPortraitMapFunction implements MapFunction<String, String> {

    private final String CollectionName = "productProfile";

    @Override
    public String map(String s) throws Exception {

        LogEntity log = JSON.parseObject(s, LogEntity.class);
        ResultSet rst = MysqlClient.selectUserById(log.getUserId());

        if (rst != null){
            while (rst.next()){
                String productId = String.valueOf(log.getProductId());
                String sex = rst.getString("sex");
                String age = rst.getString("age");
                HashMap<String,Object> result = new HashMap<>();
                result.put("productId",productId);
                result.put("sex",sex);
                result.put("age",age);
                MongoDBUtil.getInstance().addOne(result, CollectionName);
            }
        }
        return null;
    }
}
