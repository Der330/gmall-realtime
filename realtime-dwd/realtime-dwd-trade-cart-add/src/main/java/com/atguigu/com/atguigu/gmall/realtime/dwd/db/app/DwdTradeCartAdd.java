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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.codehaus.jackson.node.ContainerNode;

public class DwdTradeCartAdd {
    public static void main(String[] args) {


        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", 10013);
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


        tableEnv.executeSql(SqlUtil.readOdsDbSql(Contant.TOPIC_DWD_TRADE_CART_ADD));

        //TODO 2.过滤出加购行为
        Table cartInfoTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    if(`type`='insert',`data`['sku_num'],CAST((CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) AS string)) sku_num,\n" +
                "    ts\n" +
                "from topic_db \n" +
                "    where \n" +
                "        `table`='cart_info'\n" +
                "    and (\n" +
                "        `type`='insert' \n" +
                "        or\n" +
                "        (`type`='update' and `old`['sku_num'] is not null and (CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))\n" +
                "    )");


        //TODO 3.将加购数据写到kafka主题中
        //3.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Contant.TOPIC_DWD_TRADE_CART_ADD+" (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  sku_num string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.upsertKafkaConnector(Contant.TOPIC_DWD_TRADE_CART_ADD,4));
        //3.2 写入
        cartInfoTable.executeInsert(Contant.TOPIC_DWD_TRADE_CART_ADD);
//tableEnv.executeSql("select * from dwd_trade_cart_add");


    }
}
