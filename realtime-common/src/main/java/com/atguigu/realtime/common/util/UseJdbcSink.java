package com.atguigu.realtime.common.util;

import com.atguigu.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.realtime.common.constant.Contant;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;


public class UseJdbcSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(8089, 1);

//        DataStreamSource<String> ds=env.socketTextStream("hadoop102", 9090);
        DataStreamSource<TrafficHomeDetailPageViewBean> ds=env.fromElements(
                new TrafficHomeDetailPageViewBean(
                        new Date().toString(),
                        new Date().toString(),
                        DateFormatUtils.format(new Date(),"yyyy-MM-dd"),
                        1L,2L,123L)
        );

        SinkFunction<TrafficHomeDetailPageViewBean> jdbcSink=JdbcSink.sink(
                "insert into gmall_realtime.dws_traffic_home_detail_page_view_window values(?,?,?,?,?)",
                new JdbcStatementBuilder<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public void accept(PreparedStatement ps, TrafficHomeDetailPageViewBean data) throws SQLException {

                        ps.setString(1,"2024-09-09 09:36:39");
                        ps.setString(2,"2024-09-09 09:36:55");
                        ps.setString(3,"2024-09-09");
                        ps.setLong(4,6L);
                        ps.setLong(5,777L);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://127.0.0.1:12000/gmall_realtime")
                        .withUsername("root")
                        .withPassword(Contant.DORIS_PASSWORD)
                        .build()
        );
//
//        ds.addSink(jdbcSink);


        DataStreamSource<TradeSkuOrderBean> source=env.fromElements(
                //trademarkId=2, trademarkName=苹果, category1Id=2, category1Name=手机, category2Id=13, category2Name=手机通讯, category3Id=61, category3Name=手机, skuId=11, skuName=Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机, spuId=3, spuName=Apple iPhone 12, originalAmount=8197.0000, activityReduceAmount=0.0, couponReduceAmount=0.0, orderAmount=8197.0, orderDetailId=14283713, ts=1725934839000
                new TradeSkuOrderBean(
                        "2024-09-10 10:20:30,",
                        "2024-09-10 10:20:40,",
                        "2024-09-10",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        "2",
                        BigDecimal.ONE,
                        BigDecimal.ONE,
                        BigDecimal.ONE,
                        BigDecimal.ONE,
                        "2",
                        12L

                )
        );

        source.addSink(
                FlinkDorisUtil.jdbcSinkFunction("dws_trade_sku_order_window", TradeSkuOrderBean.class)
        );

        env.execute();


    }
}
