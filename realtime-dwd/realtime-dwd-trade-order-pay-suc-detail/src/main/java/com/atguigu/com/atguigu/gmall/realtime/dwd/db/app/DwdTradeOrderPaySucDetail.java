package com.atguigu.com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.SqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail {
    public static void main(String[] args) {



        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", 10016);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //TODO 2.并行度
        env.setParallelism(4);
        //检查点
        //开启检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //检查点配置
        CheckpointConfig checkpointConfig=env.getCheckpointConfig();
        //检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000);
        //两个检查点之间最小间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        //Job取消后是否保留检查点
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //检查点存储路径
        checkpointConfig.setCheckpointStorage(new Path("hdfs://hadoop102:8020/flink/checkpoint"));
        //重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));
        //用户
        System.setProperty("HADOOP_USER_NAME","atguigu");



        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);



        // 1. 读取下单事务事实表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 0), " +
                        "watermark for et as et - interval '3' second " +
                        ")" + SqlUtil.kafkaConnector( Contant.TOPIC_DWD_TRADE_ORDER_DETAIL,Contant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // 2. 读取 topic_db
        tableEnv.executeSql(SqlUtil.readOdsDbSql(Contant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        // 3. 读取 字典表
        tableEnv.executeSql(SqlUtil.readDicSql("dim_base_dic"));

        // 4. 从 topic_db 中过滤 payment_info
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "`pt`," +
                "ts, " +
                "et " +
                "from topic_db " +
                "where `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

        // 5. 3张join: interval join 无需设置 ttl
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");

        // 6. 写出到 kafka 中
        tableEnv.executeSql("create table "+Contant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+" (" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint ," +
                "primary key(order_detail_id) not enforced " +
                ")" + SqlUtil.upsertKafkaConnector(Contant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS,4));

        result.executeInsert(Contant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);


    }
}
