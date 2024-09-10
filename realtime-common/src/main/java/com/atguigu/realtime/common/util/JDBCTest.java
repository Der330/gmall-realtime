package com.atguigu.realtime.common.util;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JDBCTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(8089, 1);

        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);

//        -- 在 Flink SQL 中注册一张 MySQL 表 'users'

        tableEnv.executeSql("CREATE TABLE source (\n" +
                "                id int,\n" +
                "                name STRING\n" +
                "        ) WITH (\n" +
                "                'connector' = 'jdbc',\n" +
                "                'url' = 'jdbc:mysql://127.0.0.1:11000/test',\n" +
                "                'table-name' = 'user',\n" +
                "                'username' = 'root',\n" +
                "                'password' = '000000',\n" +
                "                'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                "        )")
        ;

        tableEnv.executeSql("select * from source").print();




    }
}
