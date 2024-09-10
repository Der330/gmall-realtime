package com.atguigu.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.constant.Contant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.connector.hbase.table.HBaseConnectorOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.management.RuntimeErrorException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HbaseUtil {

    //获取连接
    public static Connection getHbaseConnect() throws IOException {

        Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        conf.set("hbase.rootdir","hdfs://hadoop102:8020/hbase");

        Connection connection=ConnectionFactory.createConnection(conf);

        return connection;
    }

    //关闭连接
    public static void closeHbaseConnect(Connection con){
        try {
            con.close();
        } catch (IOException e) {
            throw new RuntimeException("hbase连接关闭失败");
        }
    }

    //获取异步操作HBase的连接对象
    public static AsyncConnection getHBaseAsyncConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            AsyncConnection asyncHBaseConn = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncHBaseConn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //关闭异步操作HBase的连接对象
    public static void closeHBaseAsyncConnection(AsyncConnection asyncHBaseConn){
        if(asyncHBaseConn != null && !asyncHBaseConn.isClosed()){
            try {
                asyncHBaseConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //Hbase建表
    public static void createHbaseTable(Connection con, String nameSpace,String tableName,String... colFamilys) throws IOException {


        try(Admin admin=con.getAdmin()) {

            TableName tableNameObj=TableName.valueOf(nameSpace, tableName);

            if (admin.tableExists(tableNameObj)) {
                new RuntimeException("[CREATE HBASE ERROR]--Hbase命名空间\""+nameSpace+"\"中\""+tableName+"\"表已存在");
            }

            if (colFamilys.length<1){
                new RuntimeException("[CREATE HBASE ERROR]--列族不能为空");
            }

            TableDescriptorBuilder descriptorBuilder=TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String colFamily : colFamilys) {
                descriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build());
            }
            admin.createTable(descriptorBuilder.build());
            System.out.println("Hbase表["+nameSpace+"."+tableName+"]创建成功");

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    //hbase删除表
    public static void dropHbaseTable(Connection con,String nameSpace, String tableName){

        try(Admin admin=con.getAdmin()) {

            TableName tableNameObj=TableName.valueOf(nameSpace, tableName);

            if (admin.tableExists(tableNameObj)) {
                admin.disableTable(tableNameObj);
                admin.deleteTable(tableNameObj);
                System.out.println("Hbase表["+nameSpace+"."+tableName+"]删除成功");
            } else {
                new RuntimeException("[DROP HBASE ERROR]--Hbase命名空间\""+nameSpace+"\"中\""+tableName+"\"不存在");
            }

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    //Hbase更新表
    public static void updateHbaseTable(Connection con, String nameSpace,String tableName,String... colFamilys) throws IOException {


        try(Admin admin=con.getAdmin()) {

            TableName tableNameObj=TableName.valueOf(nameSpace, tableName);

            if (colFamilys.length<1){
                new RuntimeException("[UPDATE HBASE ERROR]--新列族不能为空");
            }

            TableDescriptorBuilder descriptorBuilder=TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String colFamily : colFamilys) {
                descriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build());
            }
            if (admin.tableExists(tableNameObj)) {
                admin.disableTable(tableNameObj);
                admin.deleteTable(tableNameObj);
            } else {
                new RuntimeException("[UPDATE HBASE ERROR]--Hbase命名空间\""+nameSpace+"\"中\""+tableName+"\"不存在，因此不能被更新");
            }
            admin.createTable(descriptorBuilder.build());
            System.out.println("Hbase表["+nameSpace+"."+tableName+"]更新成功");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    //HbasePut行
    public static void putRow(Connection con, String nameSpace, String tableName,String rolFamily, String rowKey, JSONObject dataObj) throws IOException {

        TableName tableNameObj=TableName.valueOf(nameSpace, tableName);
        try(Table table=con.getTable(tableNameObj)) {

            Put put=new Put(Bytes.toBytes(rowKey));

            Iterator<String> keysIterator=dataObj.keySet().iterator();
            while (keysIterator.hasNext()) {
                String key=keysIterator.next();
                String value=dataObj.getString(key);
                if (value != null){
                    put.addColumn(Bytes.toBytes(rolFamily),Bytes.toBytes(key),Bytes.toBytes(value));
                }

            }

            table.put(put);
            System.out.println("向表空间"+nameSpace+"下的表"+tableName+"中put数据"+rowKey+"成功");

        } catch (Exception e){
            e.printStackTrace();
        }

    }


    //Hbase删行
    public static void deleteRow(Connection con, String nameSpace,String tableName, String rowKey) throws IOException {

        TableName tableNameObj=TableName.valueOf(nameSpace, tableName);
        try(Table table=con.getTable(tableNameObj)) {

            Delete delete=new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("向表空间"+nameSpace+"下的表"+tableName+"中删除数据"+rowKey+"成功");
        }


    }


    //根据rowkey查询数据
    public static <T> T getRow(Connection con, String nameSpace, String tableName, String rowKey, Class<T> clz, boolean ... isUnderlineToCamel) throws IOException {

        boolean defaultIsUToC = false;
        defaultIsUToC =isUnderlineToCamel.length != 0;

        TableName name = TableName.valueOf(nameSpace, tableName);
        try (Table table=con.getTable(name)){

            Get get=new Get(Bytes.toBytes(rowKey));
            Result result=table.get(get);
            List<Cell> cells=result.listCells();

            if (cells != null && cells.size() >0){
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String colName=Bytes.toString(CellUtil.cloneQualifier(cell));
                    String colValue=Bytes.toString(CellUtil.cloneValue(cell));

                    if (defaultIsUToC){
                        colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, colName);
                    }

                    BeanUtils.setProperty(obj,colName,colValue);

                }
                return obj;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;

    }

    //异步操作Hbase
    public static <T> T getRowByAsync(AsyncConnection asyncHBaseConn,  String nameSpace, String tableName, String rowKey, Class<T> clz, boolean ... isUnderlineToCamel){
        boolean defaultIsUToC = false;
        defaultIsUToC =isUnderlineToCamel.length != 0;


        try {

            TableName name = TableName.valueOf(nameSpace, tableName);
            AsyncTable<AdvancedScanResultConsumer> table=asyncHBaseConn.getTable(name);
            Get get=new Get(Bytes.toBytes(rowKey));
            Result result=table.get(get).get();
            List<Cell> cells=result.listCells();

            if (cells != null && cells.size() >0){
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String colName=Bytes.toString(CellUtil.cloneQualifier(cell));
                    String colValue=Bytes.toString(CellUtil.cloneValue(cell));

                    if (defaultIsUToC){
                        colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, colName);
                    }

                    BeanUtils.setProperty(obj,colName,colValue);

                }
                return obj;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        Connection con=getHbaseConnect();

        System.out.println(HbaseUtil.getRow(con, Contant.HBASE_NAMESPACE, "dim_base_category3", "61", JSONObject.class));
        closeHbaseConnect(con);
    }







}
