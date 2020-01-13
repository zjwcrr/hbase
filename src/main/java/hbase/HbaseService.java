package hbase;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import hbase.entity.TerminalLogHbaseBean;
import mongoDB.entity.TerminalLogEntity;
import mongoDB.enums.TerminalLogEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class HbaseService {
    /**
     * 命名空间
     */
    private static final String HBASE_BEIDOU_NAMESPACE = "beidou";
    /**
     * terminal_log表名
     */
    private static final String HBASE_TERMINAL_LOG_TABLE_NAME = "beidou:terminal_log";

    private static HbaseService INSTANCE = new HbaseService();

    private Connection connection = null;

    private HbaseService() {
        try {
            if (null == connection || connection.isClosed()) {
                System.out.println("[HbaseService]Start init hbase!");
                // 创建hbase连接
                Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "192.168.177.111");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                connection = ConnectionFactory.createConnection(configuration);
            }
        } catch (IOException e) {
            System.out.println("[HbaseService]Init hbase failed!");
        }
    }

    public static HbaseService getInstance() {
        return INSTANCE;
    }

    public void insertIntoHbase(List<TerminalLogEntity> terminalLogEntityList) throws IOException {
        // 创建表
        createTerminalLogTable(connection);

        // 表中插入数据
        instertTerminalLogTable(connection, terminalLogEntityList);
    }

    private void instertTerminalLogTable(Connection connection, List<TerminalLogEntity> terminalLogEntityList) throws IOException {
        Table table = connection.getTable(TableName.valueOf(HBASE_TERMINAL_LOG_TABLE_NAME));

        if (CollectionUtils.isEmpty(terminalLogEntityList)) {
            System.out.println("[HbaseService]TerminalLogEntityList is empty!");
            return;
        }

        int count = 0;
        for (TerminalLogEntity entity : terminalLogEntityList) {
            // 时间倒序，增加scan查询效率，优化region分区
            long baseTime = 10000000000000l;
            long time = entity.getTime();
            long reverseTime = baseTime - time;

            // 区分类型（1-终端注册，2-终端上线，3-终端下线）
            String typeName = entity.getTypeName();
            if (null == typeName || 0 == typeName.length()) {
                System.out.println("[HbaseService]TypeName is null!");
                continue;
            }
//            System.out.println("[HbaseService]TypeName is [" + typeName + "]");
            int typeValue = TerminalLogEnum.getTypeValueByName(typeName);

            // 提取信息
//            System.out.println("[HbaseService]TerminalLogEntity is: " + entity.toString());
            BasicDBObject basicDBObject = (BasicDBObject) entity.getResult();
            String plateNo = basicDBObject.getString("plateNo");
            String msg = basicDBObject.getString("msg");
            if (null == msg || 0 == msg.length()) {
                msg = basicDBObject.getString("errMsg");
            }

            String simNo = "";
            BasicDBList basicDBList = (BasicDBList) entity.getParams();
            if (null != basicDBList && basicDBList.size() > 0) {
                if (basicDBList.get(0) instanceof String) {
                    simNo = basicDBList.get(0) + "";
                } else if (basicDBList.get(0) instanceof BasicDBObject) {
                    BasicDBObject bj = (BasicDBObject) basicDBList.get(0);
//                    System.out.println("[HbaseService]Bj is: " + bj);
                    simNo = bj.getString("deviceId");
//                    System.out.println("[HbaseService]SimNo is: " + simNo);
                    if (null == simNo || 0 == simNo.length()) {
                        BasicDBObject bj1 = (BasicDBObject) bj.get("deviceInfo");
//                        System.out.println("[HbaseService]Bj1 is: " + bj1);
                        simNo = bj1.getString("deviceId");
                        if (null == simNo || 0 == simNo.length()) {
                            BasicDBObject baseInfoObj = (BasicDBObject) bj.get("deviceBaseInfo");
                            simNo = baseInfoObj.getString("deviceId");
                        }
                    }
                }
            }

            if (null == plateNo || 0 == plateNo.length()) {
                BasicDBObject object = (BasicDBObject) basicDBObject.get("body");
                if (null != object) {
                    plateNo = object.getString("carNumber");
                }
            }

            // 若plateNo为null则跳过
            if (null == plateNo || 0 == plateNo.length()) {
                System.out.println("[HbaseService]SimNo :[" + simNo + "] plateNo is null!");
                continue;
            }

            // 若simNo为null则跳过
            if (null == simNo || 0 == simNo.length()) {
                System.out.println("[HbaseService]PlateNo :[" + plateNo + "] simNo is null!");
                continue;
            }


            // 车牌号md5加密成32位唯一字符串
            String md5EncryptionPlateNo = md5Encryption(plateNo);

            // 若md5加密后为null则跳过
            if (null == md5EncryptionPlateNo || 0 == md5EncryptionPlateNo.length()) {
                System.out.println("[HbaseService]Md5EncryptionPlateNo is null!");
                continue;
            }

            // 构造rowkey
            String rowkey = reverseTime + "_" + md5EncryptionPlateNo;
            // System.out.println("[HbaseService]PlateNo:[" + plateNo + "]," + "rowkey:[" + rowkey + "]," + "simNo:[" + simNo + "]," + "time:[" + time + "]," + "typeValue:[" + typeValue + "]," + "msg:[" + msg + "]," + "count:[" + count++ + "]!");


            // 开始往hbase中插入数据
            // 插入rowkey
            Put put = new Put(Bytes.toBytes(rowkey));

            // 插入列（列簇名，列名，值）
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("plate_no"), Bytes.toBytes(plateNo));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("sim_no"), Bytes.toBytes(simNo));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("time"), Bytes.toBytes(String.valueOf(time)));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("type"), Bytes.toBytes(String.valueOf(typeValue)));
            put.addColumn(Bytes.toBytes("result_info"), Bytes.toBytes("msg"), Bytes.toBytes(msg));

            table.put(put);
        }
        System.out.println(table);
    }

    private String md5Encryption(String plateNo) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] entryMsgs = messageDigest.digest(plateNo.getBytes("utf-8"));
            return byteArrayToHexString(entryMsgs);
        } catch (NoSuchAlgorithmException e) {
            System.out.println("[HbaseService]Error Message is: " + e.getMessage());
        } catch (UnsupportedEncodingException e) {
            System.out.println("[HbaseService]Error Message is: " + e.getMessage());
        }
        return null;
    }

    private String byteArrayToHexString(byte[] entryMsgs) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < entryMsgs.length; i++) {
            int n = entryMsgs[i];
            if (n < 0) {
                n += 256;
            }
            if (n < 16) {
                // 补0
                stringBuffer.append("0");
            }
            stringBuffer.append(Integer.toHexString(n));
        }
        return stringBuffer.toString();
    }

    private void createTerminalLogTable(Connection connection) throws IOException {
        // admin管理table使用，比如创建表
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        try {
            // 查看命名空间是否存在
            NamespaceDescriptor rel = admin.getNamespaceDescriptor(HBASE_BEIDOU_NAMESPACE);
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(HBASE_BEIDOU_NAMESPACE).build();
            admin.createNamespace(namespaceDescriptor);
        }

        // 创建表名
        TableName tableName = TableName.valueOf(HBASE_TERMINAL_LOG_TABLE_NAME);

        // 判断表是否存在，存在直接返回
        if (admin.tableExists(tableName)) {
            System.out.println("[HbaseService]Table [" + tableName + "]" + " existed");
            return;
        }

        //创建列簇
        ColumnFamilyDescriptor family1 = ColumnFamilyDescriptorBuilder.of("base_info");
        ColumnFamilyDescriptor family2 = ColumnFamilyDescriptorBuilder.of("result_info");

        TableDescriptor table = TableDescriptorBuilder.newBuilder(tableName)
                .addColumnFamily(family1)
                .addColumnFamily(family2)
                .build();

        // 创建表
        admin.createTable(table);

        // 释放资源
        admin.close();
    }

    public void getTerminalLogFromHbase() throws IOException {
        Integer pageIndex = 2;
        Integer pageSize = 20;
        String plateNo = "赣B46625";
        int typeValue = 1;
        long beginTime = 1577808000000l;
        long endTime = 1578499200000l;

        long currentTime = System.currentTimeMillis();
        List<TerminalLogHbaseBean> terminalLogHbaseBeans = null;
//        // 1、范围查询所有终端类型日志
//        terminalLogHbaseBeans = scanAllLog(beginTime, endTime, pageIndex, pageSize);

        // 2、范围查询单个终端类型日志
        terminalLogHbaseBeans = scanSingleTypeLog(beginTime, endTime, typeValue, pageIndex, pageSize);
//
//        // 3、单车范围查询所有终端类型日志
//        terminalLogHbaseBeans = scanCarLog(beginTime, endTime, plateNo, pageIndex, pageSize);
//
//        //4、单车查询单个终端类型日志
//        terminalLogHbaseBeans = scanCarSingleTypeLog(beginTime, endTime, plateNo, typeValue, pageIndex, pageSize);
        long costTime = System.currentTimeMillis() - currentTime;
        System.out.println("[HbaseService]TerminalLogHbaseBeans size is: " + terminalLogHbaseBeans.size() + "}, Cost time is {" + costTime + "}");
        if (CollectionUtils.isNotEmpty(terminalLogHbaseBeans)) {
            for (TerminalLogHbaseBean bean : terminalLogHbaseBeans) {
                System.out.println("[HbaseService]TerminalLogHbaseBean is: " + bean.toString());
            }
        }
    }

    private List<TerminalLogHbaseBean> scanCarSingleTypeLog(long beginTime, long endTime, String plateNo, int typeValue, Integer pageIndex, Integer pageSize) throws IOException {
        long baseTime = 10000000000000l;
        long reverseBeginTime = baseTime - endTime;
        long reverseEndTime = baseTime - beginTime;

        // 对车牌号进行md5加密
        String md5EncryptionPlateNo = md5Encryption(plateNo);
        System.out.println("[HbaseService]PlateNo is: " + md5EncryptionPlateNo + " startRowKey is: " + reverseBeginTime + " endRowKey is: " + reverseEndTime);

        FilterList filterList = new FilterList();
        // 构造MD5车牌号查询过滤条件
        Filter md5PlateNoFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(md5EncryptionPlateNo));
        filterList.addFilter(md5PlateNoFilter);

        // 构造type查询过滤条件
        Filter typeFilter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("type"), CompareOperator.EQUAL, Bytes.toBytes(String.valueOf(typeValue)));
        filterList.addFilter(typeFilter);

        // 构造分页条件
        Filter pageFilter = new PageFilter(pageIndex * pageSize);
        filterList.addFilter(pageFilter);

        // 构造rowkey
        String startRowKey = String.valueOf(reverseBeginTime);
        String endRowKey = String.valueOf(reverseEndTime);

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setFilter(filterList);

        return scanTerminalLogFromHbase(scan, pageIndex, pageSize);
    }

    private List<TerminalLogHbaseBean> scanCarLog(long beginTime, long endTime, String plateNo, Integer pageIndex, Integer pageSize) throws IOException {
        long baseTime = 10000000000000l;
        long reverseBeginTime = baseTime - endTime;
        long reverseEndTime = baseTime - beginTime;

        // 对车牌号进行md5加密
        String md5EncryptionPlateNo = md5Encryption(plateNo);
        System.out.println("[HbaseService]PlateNo is: " + md5EncryptionPlateNo + " startRowKey is: " + reverseBeginTime + " endRowKey is: " + reverseEndTime);

        FilterList filterList = new FilterList();
        // 构造MD5车牌号查询过滤条件
        Filter md5PlateNoFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(md5EncryptionPlateNo));
        filterList.addFilter(md5PlateNoFilter);

        // 构造分页条件
        Filter pageFilter = new PageFilter(pageIndex * pageSize);
        filterList.addFilter(pageFilter);

        // 构造rowkey
        String startRowKey = String.valueOf(reverseBeginTime);
        String endRowKey = String.valueOf(reverseEndTime);

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setFilter(filterList);

        return scanTerminalLogFromHbase(scan, pageIndex, pageSize);
    }

    private List<TerminalLogHbaseBean> scanSingleTypeLog(long beginTime, long endTime, int typeValue, int pageIndex, int pageSize) throws IOException {
        long baseTime = 10000000000000l;
        long reverseBeginTime = baseTime - endTime;
        long reverseEndTime = baseTime - beginTime;


        FilterList filterList = new FilterList();
        // 构造type查询过滤条件
        Filter typeFilter = new SingleColumnValueFilter(Bytes.toBytes("base_info"), Bytes.toBytes("type"), CompareOperator.EQUAL, Bytes.toBytes(String.valueOf(typeValue)));
        filterList.addFilter(typeFilter);

        // 构造分页条件
        Filter pageFilter = new PageFilter(pageIndex * pageSize);
        filterList.addFilter(pageFilter);

        // 构造rowkey
        String startRowKey = String.valueOf(reverseBeginTime);
        String endRowKey = String.valueOf(reverseEndTime);

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setFilter(filterList);

        return scanTerminalLogFromHbase(scan, pageIndex, pageSize);
    }

    private List<TerminalLogHbaseBean> scanAllLog(long beginTime, long endTime, int pageIndex, int pageSize) throws IOException {
        long baseTime = 10000000000000l;
        long reverseBeginTime = baseTime - endTime;
        long reverseEndTime = baseTime - beginTime;

        // 构造分页条件
        FilterList filterList = new FilterList();
        Filter pageFilter = new PageFilter(pageIndex * pageSize);
        filterList.addFilter(pageFilter);

        String startRowKey = String.valueOf(reverseBeginTime);
        String endRowKey = String.valueOf(reverseEndTime);

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setFilter(filterList);

        return scanTerminalLogFromHbase(scan, pageIndex, pageSize);
    }

    private List<TerminalLogHbaseBean> scanTerminalLogFromHbase(Scan scan, int pageIndex, int pageSize) throws IOException {
        Table table = connection.getTable(TableName.valueOf(HBASE_TERMINAL_LOG_TABLE_NAME));
        ResultScanner resultScanner = table.getScanner(scan);

        // 目标偏移量
        int targetOffset = (pageIndex - 1) * pageSize;

        // 初始化偏移量
        int initOffset = 0;
        List<TerminalLogHbaseBean> terminalLogHbaseBeans = new ArrayList<TerminalLogHbaseBean>();
        for (Result result : resultScanner) {
            if (initOffset < targetOffset) {
                System.out.println("[HbaseService]InitOffset is: " + initOffset + ", targetOffset is: " + targetOffset);
                // 增加偏移量
                initOffset++;
                continue;
            }

//            // 获取各行rowKey
//            byte[] row = result.getRow();
//            System.out.println("[HbaseService]ScanAllLog row is: " + Bytes.toString(row));

            // 获取各列
            List<Cell> cells = result.listCells();
            TerminalLogHbaseBean bean = new TerminalLogHbaseBean();
            for (Cell cell : cells) {
                String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if ("plate_no".equals(key)) {
                    bean.setPlateNo(value);
                }
                if ("sim_no".equals(key)) {
                    bean.setSimNo(value);
                }
                if ("time".equals(key)) {
                    bean.setTime(Long.valueOf(value));
                }
                if ("type".equals(key)) {
                    bean.setType(Integer.valueOf(value));
                }
                if ("msg".equals(key)) {
                    bean.setMsg(value);
                }
            }
            terminalLogHbaseBeans.add(bean);
        }
        return terminalLogHbaseBeans;
    }
}
