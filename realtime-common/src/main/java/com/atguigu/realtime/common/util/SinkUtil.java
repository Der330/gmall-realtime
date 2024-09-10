package com.atguigu.realtime.common.util;

import com.atguigu.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.realtime.common.constant.Contant;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkUtil {
    public static SinkFunction<TrafficPageViewBean> getJdbcSinkFunction(String tableName, TrafficPageViewBean viewBean ){


        SinkFunction<TrafficPageViewBean> jdbcSink=JdbcSink.sink(

                "insert into "+ Contant.DORIS_DATABASE +"."+tableName+" values(?,?,?,?,?,?,?,?,?,?,?)",
                new JdbcStatementBuilder<TrafficPageViewBean>() {

                    @Override
                    public void accept(PreparedStatement ps, TrafficPageViewBean viewBean) throws SQLException {
                        ps.setString(1,viewBean.getSid());
                        ps.setString(2,viewBean.getEdt());
                        ps.setString(3,viewBean.getCur_date());
                        ps.setString(4,viewBean.getVc());
                        ps.setString(5,viewBean.getCh());
                        ps.setString(6,viewBean.getAr());
                        ps.setString(7,viewBean.getIsNew());
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/fruit")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        return jdbcSink;
    }
}
