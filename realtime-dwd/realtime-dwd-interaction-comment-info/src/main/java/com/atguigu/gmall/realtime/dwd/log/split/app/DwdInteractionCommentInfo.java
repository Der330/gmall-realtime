package com.atguigu.gmall.realtime.dwd.log.split.app;

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


/*
    1.读取topic_db内容
    2.读取Hbase里dic_dim内容
    3.用lookupJoin合并做维度退化
    4.将结果写入topic_comment
 */


public class DwdInteractionCommentInfo {
    public static void main(String[] args) {

        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", 10012);
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

        tableEnv.executeSql("CREATE TABLE commentSource (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>,\n" +
                "  `ts` BIGINT,\n" +
                "  `pt` as proctime(),\n" +
                "  `et` as TO_TIMESTAMP_LTZ(ts, 0),\n" +
                "WATERMARK FOR et AS et\n" +
                ")" +
                SqlUtil.kafkaConnector(Contant.TOPIC_DB,Contant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );



        Table commentTable=tableEnv.sqlQuery("select\n" +
                "    data['id'] `id`,\n" +
                "    data['user_id'] `user_id`,\n" +
                "    data['sku_id'] `sku_id`,\n" +
                "    data['appraise'] `appraise`,\n" +
                "    data['comment_txt'] `comment_txt`,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from commentSource where `table` = 'comment_info' and type in ('insert','update')\n"
        );
        tableEnv.createTemporaryView("commentTable", commentTable);


        tableEnv.executeSql(
                "CREATE TABLE dicSource (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'hadoop102:2181',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '2000',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ");"
        );

        Table commentDwdTable=tableEnv.sqlQuery("SELECT \n" +
                "    c.id id,\n" +
                "    c.user_id user_id,\n" +
                "    c.sku_id sku_id,\n" +
                "    c.appraise appraise,\n" +
                "    d.dic_name appraise_name,\n" +
                "    c.comment_txt comment_txt,\n" +
                "    c.ts ts\n" +
                "FROM commentTable AS c\n" +
                "JOIN dicSource FOR SYSTEM_TIME AS OF c.pt AS d\n" +
                "    ON c.appraise = d.dic_code"
        );
        tableEnv.createTemporaryView("commentDwdTable", commentDwdTable);

        tableEnv.executeSql("CREATE TABLE "+Contant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id  string,\n" +
                "    user_id string,\n" +
                "    sku_id  string,\n" +
                "    appraise    string,\n" +
                "    appraise_name    string,\n" +
                "    comment_txt string,\n" +
                "    ts  bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" +
                SqlUtil.upsertKafkaConnector(Contant.TOPIC_DWD_INTERACTION_COMMENT_INFO,4)
        );

        commentDwdTable.executeInsert(Contant.TOPIC_DWD_INTERACTION_COMMENT_INFO);




    }
}
