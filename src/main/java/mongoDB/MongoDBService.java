package mongoDB;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import mongoDB.entity.TerminalLogEntity;
import mongoDB.utils.MongoBeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class MongoDBService {
    public static List<TerminalLogEntity> initMongoDB() {
        // mongoDB的ip和port
        List<ServerAddress> adds = new ArrayList<ServerAddress>();
        ServerAddress serverAddress = new ServerAddress("192.168.4.10", 20010);
        adds.add(serverAddress);

        // mongoDB的用户名和密码
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential("hlet", "hlet_beidou_busi", "hlet123456".toCharArray());
        credentials.add(mongoCredential);

        // mongoDB的其它设置
        MongoClientOptions options = MongoClientOptions.builder()
                .connectionsPerHost(1000 * 30)
                .maxWaitTime(1000 * 30)
                .build();

        // 建立mongodb服务连接
        MongoClient mongoClient = new MongoClient(adds, credentials, options);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("hlet_beidou_busi");
        if (null == mongoDatabase) {
            System.out.println("[MongoDBService]InitMongoDB mongoDatabase is null!");
            return null;
        }
        System.out.println("[MongoDBService]InitMongoDB connected to mongodb successfully!");
        final List<TerminalLogEntity> resultList = new ArrayList<TerminalLogEntity>();
        Block<DBObject> handler = new Block<DBObject>() {
            public void apply(DBObject dbObject) {
//                System.out.println("[DemoMain]DBObject is : " + dbObject);
                TerminalLogEntity entity = new TerminalLogEntity();
                try {
                    MongoBeanUtils.dbObjectToBean(dbObject, entity);
                    resultList.add(entity);
                } catch (InvocationTargetException e) {
                    System.out.println("[MongoDBService]InitMongoDB InvocationTargetException: " + e.getMessage());
                } catch (IllegalAccessException e) {
                    System.out.println("[MongoDBService]InitMongoDB IllegalAccessException is: " + e.getMessage());
                }
            }
        };

        // 构造查询条件（只查询终端注册日志）
//        BasicDBObject query = filterBasicDBList();

        long beginTime = System.currentTimeMillis();
        System.out.println("[MongoDBService]Begin query currentTime is : " + beginTime);
        // 查询
        FindIterable<DBObject> findIt = mongoDatabase.getCollection("bd_terminal_log", DBObject.class).find();
        findIt = findIt.skip(0);
//        findIt = findIt.limit(Integer.MAX_VALUE);
        findIt = findIt.limit(Integer.MAX_VALUE);
        findIt.forEach(handler);

        long costTime = System.currentTimeMillis() - beginTime;
        System.out.println("[MongoDBService]ResultList size is : {" + resultList.size() + "}, Cost time is {" + costTime + "}");
//        if (CollectionUtils.isNotEmpty(resultList)) {
//            for (TerminalLogEntity terminalLogEntity : resultList) {
//                System.out.println("[DemoMain]TerminalLogEntity is : " + terminalLogEntity.toString());
//            }
//        }
        return resultList;
    }

    private static BasicDBObject filterBasicDBList() {
        BasicDBObject sort = new BasicDBObject();

        BasicDBObject groupConfig = new BasicDBObject();
        groupConfig.put("time", -1);
        sort.put("$sort", groupConfig);
        return sort;
    }
}
