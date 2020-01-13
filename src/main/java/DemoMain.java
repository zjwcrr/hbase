import hbase.HbaseService;
import mongoDB.MongoDBService;
import mongoDB.entity.TerminalLogEntity;

import java.io.IOException;
import java.util.List;


public class DemoMain {
    public static void main(String[] args) throws IOException {
//        // 1、从mongDB过滤查询出所有的终端注册日志记录
//        List<TerminalLogEntity> terminalLogEntityList = MongoDBService.initMongoDB();
//
//        // 2、将记录存入hbase
//        HbaseService.getInstance().insertIntoHbase(terminalLogEntityList);

        // 3、查询hbase
        HbaseService.getInstance().getTerminalLogFromHbase();
    }
}
