package flink.utils;

import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.*;

import lombok.extern.java.Log;

import static com.mongodb.client.model.Filters.eq;

@Log
public class MongoDBUtil {

    private MongoClient client = null;
    private MongoDatabase database = null;
    private static String host ="192.168.56.10";
    private static String port ="27017";
    private static String dbName ="admin";


    /**
     * 静态内部类单例模式实现
     */
    private static class MySingleHandler {
        private static MongoDBUtil instance = new MongoDBUtil();
    }

    public static MongoDBUtil getInstance(){
        return MySingleHandler.instance;
    }

    /**
     * 初始化，从配置文件中读取MongoDB地址、端口、数据库进行连接
     */
    private MongoDBUtil() {
        try {
            String url = "mongodb://" + host + ":" + port;
            client = MongoClients.create(url);
            database = client.getDatabase(dbName);
            log.info("MongoDB connect successful!");
        }catch (Exception e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    /**
     * 单条数据插入
     * @param data 数据
     * @param collectionName collection名
     */
    public void addOne(Map<String, Object> data, String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(new Document(data));
    }

    /**
     * 多条数据插入
     * @param maps 数据列表
     * @param collectionName 集合名
     */
    public void addMany(List<Map<String, Object>> maps, String collectionName) {
        List<Document> documents = new ArrayList<Document>();
        for(Map<String, Object> map : maps) {
            documents.add(new Document(map));
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertMany(documents);
    }

    /**
     * 单条数据替换（更新）,不存在时可以强制插入
     * @param key 需要替换数据的字典中的key（对应MySQL表的字段名)
     * @param value key对应的值(对应MySQL表的字段值）
     * @param newMap 完整的新的数据
     * @param collectionName collection（表）名称
     * @param isInsert 如果数据库中不存在此数据，是否插入当条数据到数据库中
     */
    public void replaceOne(String key, Object value, Map<String, Object> newMap, String collectionName, boolean isInsert) {
        Bson filter = eq(key, value);
        String mongoIdKey = "_id";
        if (key.equals(mongoIdKey)) {
            filter = eq(key, new ObjectId((String) value));
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        UpdateResult result = collection.replaceOne(filter, new Document(newMap));
        if(result.getMatchedCount() == 0 && isInsert) {
            MongoDBUtil.getInstance().addOne(newMap, collectionName);
        }
    }

    /**
     * 单条数据更新，多添加筛选更新，不存在时可以强制插入
     * @param condition 更新添加
     * @param map 完整的新的数据
     * @param collectionName collection（表）名称
     * @param isInsert 如果数据库中不存在此数据，是否插入当条数据到数据库中
     */
    public void replaceOne(Document condition, Map<String, Object> map, String collectionName, boolean isInsert) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        UpdateResult result = collection.replaceOne(condition, new Document(map));
        if(result.getMatchedCount() == 0 && isInsert) {
            MongoDBUtil.getInstance().addOne(map, collectionName);
        }
    }

    /**
     * 单条数据删除
     * @param key 需要数据数据的字典中的key（对应MySQL表的字段名)
     * @param value key对应的值(对应MySQL表的字段值）
     * @param collectionName collection（表）名称
     */
    public void deleteOne(String key, Object value, String collectionName) {
        Bson filter = eq(key, value);
        String mongoIdKey = "_id";
        if (key.equals(mongoIdKey)) {
            filter = eq(key, new ObjectId((String) value));
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteOne(filter);
    }

    /**
     * 单条数据删除(多筛选添加删除)
     * @param condition 删除筛选条件
     * @param collectionName collection（表）名称
     */
    public void deleteOne(Document condition, String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteOne(condition);
    }

    /**
     * 多条数据删除
     * @param key 需要数据数据的字典中的key（对应MySQL表的字段名)
     * @param value key对应的值(对应MySQL表的字段值）
     * @param collectionName collection（表）名称
     */
    public void deleteMany(String key, Object value, String collectionName) {
        Bson filter = eq(key, value);
        String mongoIdKey = "_id";
        if (key.equals(mongoIdKey)) {
            filter = eq(key, new ObjectId((String) value));
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteMany(filter);
    }

    /**
     * collection删除
     * @param collectionName 集合名
     */
    public void dropCollection(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.drop();
    }

    /**
     * 获取当前collection中文档的数量
     * @param collectionName 集合名
     * @param condition 筛选条件
     * @return 文档数量
     */
    public long count(String collectionName, Map<String, Object> condition) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.countDocuments(new Document(condition));
    }

    /**
     * 查询：自行构造丰富的多条件查询
     * @param condition 查询条件
     * @param collectionName collection（表）名称
     * @return 数据列表
     */
    public FindIterable<Document> query(Document condition, String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.find(condition);
    }

    /**
     * 查询：单字段查询
     * @param key 字段名
     * @param value 字段值
     * @param collectionName collection（表）名称
     * @return 单条数据（如果查询出多个，也只返回一个）
     */
    public Document queryOne(String key, Object value, String collectionName) {
        Bson filter = eq(key, value);
        String mongoIdKey = "_id";
        if (key.equals(mongoIdKey)) {
            filter = eq(key, new ObjectId((String) value));
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.find(filter).first();
    }

    /**
     * 2023-4-20修改 新增查询多条数据
     */
    public List<String> queryManyList(String key, Object value, String columnName,String collectionName) {
        Bson filter = eq(key, value);
        String mongoIdKey = "_id";
        if (key.equals(mongoIdKey)) {
            filter = eq(key, new ObjectId((String) value));
        }
        List<String> result = new ArrayList<>();
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> documents= collection.find(filter);
        for (Document document : documents){
            result.add(document.getString(collectionName));
        }
        return result;
    }

    /**
     * 建立索引
     * @param collectionName 集合表名
     * @param key 索引名
     * @param sort 升降序 1 升序 -1 降序
     */
    public void createIndex(String collectionName, String key, int sort) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.createIndex(new Document(key, sort));
    }

    public static void main(String[] args){

        // 单条插入测试
        Map map = new HashMap() {{
            put("name", "testxuyaohui01");
        }};
        MongoDBUtil.getInstance().addOne(map, "productProfile");

    }
}

