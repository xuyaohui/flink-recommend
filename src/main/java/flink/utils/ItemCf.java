package flink.utils;


import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 基于协同过滤的产品相关度计算
 * * 策略1 ：协同过滤
 *      *           abs( i ∩ j)
 *      *      w = ——————————————
 *      *           sqrt(i || j)
 */
public class ItemCf {

    public static void getSingelItemCfCoeff(String id, List<String> others) throws Exception {

        for (String other : others) {
            if(id.equals(other)) continue;
            Double score = twoItemCfCoeff(id, other);
            HashMap result = new HashMap();
            result.put("product_id",id);
            result.put("other_product_id",other);
            result.put("score",score.toString());
            MongoDBUtil.getInstance().addOne(result, "px");
        }
    }

    private static Double twoItemCfCoeff(String id, String other) throws IOException {
        List<String> productUsers= MongoDBUtil.getInstance().queryManyList("product_id",id,"user_id","p_history");
        List<String> otherProductUsers= MongoDBUtil.getInstance().queryManyList("product_id",other,"user_id","p_history");

        int n = productUsers.size();
        int m = otherProductUsers.size();
        int sum = 0;
        Double total = Math.sqrt(n * m);
        for (String key : productUsers) {
            for (String otherKey : otherProductUsers) {
                if (key.equals(otherKey)) {
                    sum++;
                }
            }
        }
        if (total == 0){
            return 0.0;
        }
        return sum/total;

    }

    /**
     * 这里都是测试数据，用于计算两个产品的相似度，其中"1"和"2"都是产品ID，执行后会插入到mongodb的px表
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        List<String> otherProductList = new ArrayList<>();
        //这里输入产品id
        otherProductList.add("2");
        //otherProductList.add("2");
        //otherProductList.add("2");
        //otherProductList.add("2");

        getSingelItemCfCoeff("1",otherProductList);
    }
}
