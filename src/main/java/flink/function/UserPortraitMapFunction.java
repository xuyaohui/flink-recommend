package flink.function;

import com.alibaba.fastjson.JSON;
import flink.domin.LogEntity;
import flink.utils.MongoDBUtil;
import flink.utils.MysqlClient;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.ResultSet;
import java.util.HashMap;

/**
 *
 */
public class UserPortraitMapFunction implements MapFunction<String, String> {

    private final String CollectionName = "userProfile";
    //新增产品-用户表，用于后续计算产品之间相似度
    private final String P_HISTORY = "p_history";

    @Override
    public String map(String s) throws Exception {

        LogEntity log = JSON.parseObject(s, LogEntity.class);
        ResultSet rst = MysqlClient.selectById(log.getProductId());


        if (rst != null) {
            while (rst.next()) {

                String userId = String.valueOf(log.getUserId());

                String country = rst.getString("country");
                String color = rst.getString("color");
                String style = rst.getString("style");

                HashMap result = new HashMap();
                result.put("userId", userId);
                result.put("country", country);
                result.put("color", color);
                result.put("style", style);

                MongoDBUtil.getInstance().addOne(result, CollectionName);

                HashMap pHistory= new HashMap<>();
                pHistory.put("product_id",log.getProductId());
                pHistory.put("user_id",log.getUserId());
                MongoDBUtil.getInstance().addOne(pHistory, P_HISTORY);

            }
        }
        return null;
    }
}
