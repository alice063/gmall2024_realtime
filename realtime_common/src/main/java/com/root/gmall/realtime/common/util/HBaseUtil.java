package com.root.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HBaseUtil {
    public static Connection getConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }
    public static void closeConnection(Connection connection){
        if (connection!=null && !connection.isClosed()){
            try{
                connection.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "47.94.196.219");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public static void createTable(Connection connection,String namespace,String table,String... families) throws IOException {
        if( families == null || families.length ==0 ){
            return;
        }
        //1.获取admin
        Admin admin = connection.getAdmin();
        //2.创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
        for (String family:families){
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        //3.使用admin调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        }catch (Exception e){
            //e.printStackTrace();
            System.out.println("当前表格已经存在，不需要重复创建"+namespace+":"+table);
        }
        //4.关闭admin
        admin.close();

    }
    public static void dropTable(Connection connection,String namespace,String table) throws IOException {
        //1.获取admin
        Admin admin = connection.getAdmin();
        //2.调用方法删除表格
        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        }catch (Exception e){
            e.printStackTrace();
        }
        //3.关闭admin
        admin.close();
    }

    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if(columnValue != null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(columnValue));
            }
        }
        //3.调用方法写出数据
        try {
            table.put(put);
        }catch (IOException e){

            e.printStackTrace();
        }
        //4.关闭table
        table.close();
    }

    public static JSONObject getCells(Connection connection,String namespace,String tableName,String rowKey) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        JSONObject jsonObject = new JSONObject();
        //3.调用get方法
        try {
            Result result = table.get(get);
            for (Cell cell:result.rawCells()){
                jsonObject.put(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8),new String(CellUtil.cloneValue(cell),StandardCharsets.UTF_8));
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        //关闭table
        table.close();
        return jsonObject;
    }

    public static JSONObject getAsyncCells(AsyncConnection hBaseAsyncConn, String nameSpace, String tableName, String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> table = hBaseAsyncConn.getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = table.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //3.调用方法删除数据
        try{
            table.delete(delete);
        }catch (IOException e){
            e.printStackTrace();
        }
        //4.关闭table
        table.close();
    }
}
