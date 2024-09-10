package com.com.atguigu.gmall.realtime.dwd.db.app;

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

public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) {


        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", 10015);
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

        tableEnv.executeSql(SqlUtil.readOdsDbSql(Contant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 2. 读取 dwd 层下单事务事实表数据
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
                        "ts bigint " +
                        ")" + SqlUtil.kafkaConnector(Contant.TOPIC_DWD_TRADE_ORDER_DETAIL,Contant.TOPIC_DWD_TRADE_ORDER_CANCEL));

        // 3. 从 topic_db 过滤出订单取消数据
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where  `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 4. 订单取消表和下单表进行 join
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "oc.ts " +
                        "from dwd_trade_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");

        // 5. 写出
        tableEnv.executeSql(
                "create table "+Contant.TOPIC_DWD_TRADE_ORDER_CANCEL+" (" +
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
                        "cancel_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint ," +
                        "primary key(id) not enforced " +
                        ")" + SqlUtil.upsertKafkaConnector(Contant.TOPIC_DWD_TRADE_ORDER_CANCEL,4));

        result.executeInsert(Contant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }
}
