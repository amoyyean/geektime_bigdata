package com.geektime.linyan.hbase;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.web.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

//import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class JavaAPI {
    private static final String tableName = "linyan:student";//表名
    private static final String colFamily1 = "info";//列族名1
    private static final String colFamily2 = "score";//列族名2
    private static final String rowKey1 = "Tom";//行号1
    private static final String rowKey2 = "Jerry";//行号2
    private static final String rowKey3 = "Jack";//行号3
    private static final String rowKey4 = "Rose";//行号4
    private static final String rowKey5 = "Linyan";//行号5
    private static final String col1 = "student_id";//列名1
    private static final String col2 = "class";//列名2
    private static final String col3 = "understanding";//列名3
    private static final String col4 = "programming";//列名4

    private static final Logger logger = LoggerFactory.getLogger(JavaAPI.class);

    public static void main(String[] args) throws IOException {
        //创建HBase配置对象
        Configuration conf = HBaseConfiguration.create();
        //Configuration conf = new Configuration();
        //指定ZooKeeper集群地址
        conf.set("hbase.zookeeper.quorum","jikehadoop01,jikehadoop02,jikehadoop03");
        //conf.set("hbase.zookeeper.quorum", "172.16.63.17,172.16.63.14,172.16.63.15"); // 47.101.206.249,47.101.216.12,47.101.204.23
        //指定ZooKeeper集群端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //创建连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        //指定HMaster
        //conf.set("hbase.master", "master:16000"); //jikehadoop02
        //指定HBase在HDFS上的存储地址
        //conf.set("hbase.rootdir", "hdfs://master:8020/hbase");

        //创建新表
        createTable(conn, tableName, colFamily1, colFamily2);
        //createTable(conn, tableName, "info", "score");
        logger.info("创建表 {}", tableName);

        //写入数据
        put(conn, tableName, rowKey1, colFamily1, col1, "20210000000001");
        put(conn, tableName, rowKey1, colFamily1, col2, "1");
        put(conn, tableName, rowKey1, colFamily2, col3, "75");
        put(conn, tableName, rowKey1, colFamily2, col4, "82");
        put(conn, tableName, rowKey2, colFamily1, col1, "20210000000002");
        put(conn, tableName, rowKey2, colFamily1, col2, "1");
        put(conn, tableName, rowKey2, colFamily2, col3, "85");
        put(conn, tableName, rowKey2, colFamily2, col4, "67");
        put(conn, tableName, rowKey3, colFamily1, col1, "20210000000003");
        put(conn, tableName, rowKey3, colFamily1, col2, "2");
        put(conn, tableName, rowKey3, colFamily2, col3, "80");
        put(conn, tableName, rowKey3, colFamily2, col4, "80");
        put(conn, tableName, rowKey4, colFamily1, col1, "20210000000004");
        put(conn, tableName, rowKey4, colFamily1, col2, "2");
        put(conn, tableName, rowKey4, colFamily2, col3, "60");
        put(conn, tableName, rowKey4, colFamily2, col4, "61");
        put(conn, tableName, rowKey5, colFamily1, col1, "G20210735010100");
        put(conn, tableName, rowKey5, colFamily1, col2, "1");
        put(conn, tableName, rowKey5, colFamily2, col3, "90");
        put(conn, tableName, rowKey5, colFamily2, col4, "81");

        //logger.info("写入数据.");

        //查询数据
        getFamily(conn, tableName, rowKey5, colFamily2);
        //logger.info("读取单元格-{}.{}.{}:{}", rowKey5, colFamily2, col3, value);

        List<Map<String, String>> dataList = scan(conn, tableName, rowKey3, rowKey1);
        logger.info("扫描表结果-:\n{}", (dataList));

        List<Map<String, String>> dataList2 = scan(conn, tableName, rowKey3, rowKey4);
        logger.info("扫描表结果-:\n{}", (dataList2));

        //删除表
        deleteTable(conn, tableName);
        logger.info("删除表 {}", tableName);
    }

    /**
     * 创建表
     *
     * @param conn 连接
     * @param tName 表名
     * @param familyNames 列族名
     * @throws IOException IO异常
     */
    public static void createTable(Connection conn, String tName, String... familyNames) throws IOException {
        Admin admin = null;
        try {
            //得到数据库管理员对象
            admin = conn.getAdmin();
            //指定表名
            TableName tableName = TableName.valueOf(tName);
            //判断表是否存在
            boolean tbl_exs = admin.tableExists(tableName);
            if (tbl_exs) {
                System.out.println(tableName + "表已存在");
            } else {
                //判断是否可以获取
                //if (!admin.isTableAvailable(tableName)) {
                //构造表描述器
                TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
                //HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                //HTableDescriptor ht = new HTableDescriptor(tableName);
                //构造列族描述器
                for (String familyName : familyNames) {
                    //ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
                    //ColumnFamilyDescriptor cfd = cdb.build();
                    //tdb.setColumnFamily(cfd);
                    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName));
                    //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
                    //HColumnDescriptor familyDescriptor = new HColumnDescriptor(familyName);
                    //tableDescriptor.addFamily(familyDescriptor);
                }
                //获得表描述器
                TableDescriptor td = tdb.build();
                //创建表
                admin.createTable(td);
                //logger.info("create table:{} success!", tableName.getName());
                System.out.println(tableName + "表已创建");
                //}
                }
            }
        finally {
            if (admin != null) {
                admin.close();
            }
        }

//        if(!admin.tableExists(tableName)) {
//            admin.createTable(td);
//        }
//        else {
//            if(!admin.isTableDisabled(tableName)) {
//                admin.disableTable(tableName);
//            }
//            admin.deleteTable(tableName);
//            System.out.println(tableName+"表已删除");
//            admin.createTable(td);
//        }
//        System.out.println(tableName+"表已创建");
    }


    /**
     * 插入数据
     *
     * @param conn 连接
     * @param tName 表名
     * @param rowKey 行键
     * @param familyName 列族名
     * @param column 列标识
     * @param data 数据值
     * @throws IOException IO异常
     */
    public static void put(Connection conn, String tName, String rowKey, String familyName,
                           String column, String data) throws IOException {
        //建立表连接
        TableName tableName = TableName.valueOf(tName);
//        Table table = null;
//        try {
        Table table = conn.getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(data));
        table.put(put);

//         Put put = new Put(rowKey1.getBytes());
//         put.addColumn(colFamily1.getBytes(), col1.getBytes(), "20210000000001".getBytes());
//         put.addColumn(colFamily1.getBytes(), col2.getBytes(), "1".getBytes());
//         table.put(put);

        System.out.println("插入数据成功");
//        }
//        finally {
//            if (table != null) {
//                table.close();
//            }
//        }
    }

    /**
     * 根据row key, column family 读取
     *
     * @param conn 连接
     * @param tName 表名
     * @param rowKey 行键
     * @param familyName 列族名
     * @throws IOException IO异常
     */
    public static void getFamily(Connection conn, String tName, String rowKey, String familyName) throws IOException {
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(tName);
            table = conn.getTable(tableName);
//          Get get = new Get(rowKey1.getBytes());
//          get.addFamily(colFamily1.getBytes());
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(familyName));

            Result result = table.get(get);

            for (Cell cell:result.rawCells()) {
                System.out.print(new String(CellUtil.cloneRow(cell))+'\t');
                System.out.print(new String(CellUtil.cloneFamily(cell))+':');
                System.out.print(new String(CellUtil.cloneQualifier(cell))+' ');
                System.out.print(new String(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * 扫描全表的内容
     *
     * @param conn 连接
     * @param tName 表名
     * @param rowKeyStart 检索起始行键
     * @param rowKeyEnd 检索终止行键
     * @throws IOException IO异常
     */
    public static List<Map<String, String>> scan(Connection conn, String tName, String rowKeyStart, String rowKeyEnd) throws IOException {
        Table table = null;
        try {
            TableName tableName = TableName.valueOf(tName);
            table = conn.getTable(tableName);
            ResultScanner scanner = null;
            try {
                Scan scan = new Scan();
                if (!StringUtils.isEmpty(rowKeyStart)) {
                    scan.withStartRow(Bytes.toBytes(rowKeyStart));
                }
                if (!StringUtils.isEmpty(rowKeyEnd)) {
                    scan.withStopRow(Bytes.toBytes(rowKeyEnd));
                }
                scanner = table.getScanner(scan);
                Iterator<Result> results = scanner.iterator();
                while (results.hasNext()) {
                    Result result=results.next();
                    for (Cell cell:result.rawCells()) {
                        System.out.print(new String(CellUtil.cloneRow(cell))+'\t');
                        System.out.print(new String(CellUtil.cloneFamily(cell))+':');
                        System.out.print(new String(CellUtil.cloneQualifier(cell))+' ');
                        System.out.print(new String(CellUtil.cloneValue(cell)));
                        System.out.println();
                    }
                }

                List<Map<String, String>> dataList = new ArrayList<>();
                for (Result r : scanner) {
                    Map<String, String> objectMap = new HashMap<>();
                    for (Cell cell : r.listCells()) {
                        String qualifier = new String(CellUtil.cloneQualifier(cell));
                        String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                        objectMap.put(qualifier, value);
                    }
                    dataList.add(objectMap);
                }
                return dataList;
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
//    //row key 区间为左闭右开
//    scan.withStartRow(rowKey2.getBytes());
//    scan.withStopRow(rowKey5.getBytes());

    /**
     * 删除表
     *
     * @param conn 连接
     * @param tName 表名
     * @throws IOException IO异常
     */
    public static void deleteTable(Connection conn, String tName) throws IOException {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(tName);
            if (admin.tableExists(tableName)) {
                //先disable表
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName + "表已删除");
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

}
