package com.minboyin.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by minbo on 2021/2/27 20:35
 *
 */

public class HBaseOperator {
    public static void main(String[] args) throws Exception {
        //System.out.println(isTableExist("student"));
        //createTable("student2","info","course","scores");
        //dropTable("student");
        //addData("student2","1001","info","name","minbo");
        //addData("student2","1001","scores","math","99");
        //addData("student2","1002","info","name","lily");
        //addData("student2","1002","scores","physical","98");
        //getAllDate("student2");
        //getRowData("student2","1001");
        //getRowQualifierData("student2","1001","scores","math");
        //deleteRowData("student2","1001");
        //boolean flag=isExistColumnFamily("student2","info");
        //System.out.println(flag);
        descTable("student2");
        getAllColumnFamilies("student2");
    }

    /**
     * 获取configuration
     */

    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","bigdata1:2181,bigdata2:2181,bigdata3:2181");
    }

    /**
     * 1、判断表是否存在
     * @return
     */

    public static boolean isTableExist(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * 2、判断表是否包含指定的列簇
     * @return
     */
    public static boolean isExistColumnFamily(String tableName,String cf) throws Exception {
        //判断给定的tableName是否存在
        if (isTableExist(tableName)){
            // 创建HTableDescriptor类对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 获取 列簇为“cf"的详细信息
            HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(cf)){
                    System.out.println("cf:"+ columnFamily.getNameAsString());
                    return true;
                }
            }
            return false;
        }else {
            System.out.println("table:"+tableName+"不存在");
            return false;
        }
    }

    /**
     * 3、获取表所有的列簇
     * @return
     */
    public static void descTable(String tableName) throws Exception {
        //判断表是否存在
        if(isTableExist(tableName)) {
            //获取表中列簇的描述信息
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //获取列簇中列的信息
            HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
            for(HColumnDescriptor columnFamily : columnFamilies) {
                byte[] name = columnFamily.getName();
                String value = Bytes.toString(name);
                System.out.println(value);
            }
        }else {
            System.out.println("table不存在");
        }

    }
    /**
     * 4、获取表所有的列簇
     * @return
     */

    public static void getAllColumnFamilies(String tableName) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        HTableDescriptor tableDescriptor = table.getTableDescriptor();
        for (HColumnDescriptor columnFamily : tableDescriptor.getColumnFamilies()) {
            System.out.println(columnFamily.getNameAsString());
        }
    }

    /**
     * 5、创建表，
     */
    public static void createTable(String tableName,String... columnFamily) throws Exception {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        //判断表是否存在
        if (isTableExist(tableName)){
            System.out.println("table:"+tableName+"已存在");
        }
        else {
            // 创建表属性对象，表名需要转字节类型
            // HTableDescriptor：所有列簇的消息
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建多个列簇
            for (String cf : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            // 根据表的配置，创建表
            hBaseAdmin.createTable(hTableDescriptor);
            System.out.println("table:"+tableName+"创建成功");
        }

    }

    /**
     * 6、删除表：需要判断表是否存在；存在：需要disable、然后drop
     */
    public static void dropTable(String tableName) throws Exception {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        
        //判断表是否存在
        if (isTableExist(tableName)){
            
            // disable表,然后删除表
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("table:"+tableName+"已删除");
        }else {
            System.out.println("table:"+tableName+"不存在");
        }

    }

    /**
     * 7、插入数据
     */
    public static void addData(String tableName,String rowKey,String columnFamily,String columnQualifier,String value) throws Exception {
        
        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        
        //向put对象中组装数据
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("数据插入成功");
    }

    /**
     * 8、获取所有数据 getAllDate
     */
    public static void getAllDate(String tableName) throws Exception {
        
        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        
        //得到用于扫描region的对象scan
        Scan scan = new Scan();
        
        //使用HTable得到resultScanner类对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        
        for (Result result : resultScanner) {
            
            //获取每一个result中的cells信息
            Cell[] cells = result.rawCells();
            
            //循环遍历输出cells中的详细信息
            for (Cell cell : cells) {
                System.out.println("行键："+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列簇："+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列限定符："+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
                
            }
            
        }
    }

    /**
     * 9、获取一行数据 getRowDate
     */
    public static void getRowData(String tableName,String rowKey) throws Exception {
        
        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        
        //创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        
        //使用hTable得到result类对象
        Result result = hTable.get(get);
        
        //获取result中的cells信息
        Cell[] cells = result.rawCells();
        
        //循环遍历cells中的详细信息
        for (Cell cell : cells) {
            System.out.println("行键："+Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列簇："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列限定符："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: " + cell.getTimestamp());
        }

    }

    /**
     * 10、获取一行中指定列限定符的数据 getRowQualifierDate
     */
    public static void getRowQualifierData(String tableName,String rowKey,String columnFamily,String columnQualifier) throws Exception {

        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);

        //创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //columnFamily参数、columnQualifier参数传递给get对象
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier));

        //创建result类对象
        Result result = hTable.get(get);

        //获取result对象中的cells
        Cell[] cells = result.rawCells();

        //迭代遍历cells中的详细信息
        for (Cell cell : cells) {
            System.out.println("行键："+Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列簇："+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列限定符："+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
        }

    }

    /**
     * 11、删除单行或者多行数据：不定长参数
     */
    public static void deleteRowData(String tableName,String... rowKeys) throws Exception {

        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);

        //获取待删除的列表
        ArrayList<Delete> deletes = new ArrayList<>();

        //循环
        for (String rowKey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            deletes.add(delete);
        }

        //HTable删除deleteList
        hTable.delete(deletes);
        hTable.close();
        System.out.println("删除成功");
    }

}
