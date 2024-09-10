package com.atguigu.realtime.common.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvUtil {
    public static StreamExecutionEnvironment getEnv(int port,int parallelism ){

        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //TODO 2.并行度
        env.setParallelism(parallelism);
        //检查点
//        //开启检查点
//        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
//        //检查点配置
//        CheckpointConfig checkpointConfig=env.getCheckpointConfig();
//        //检查点超时时间
//        checkpointConfig.setCheckpointTimeout(60000);
//        //两个检查点之间最小间隔时间
//        checkpointConfig.setMinPauseBetweenCheckpoints(9000);
//        //Job取消后是否保留检查点
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //检查点存储路径
//        checkpointConfig.setCheckpointStorage(new Path("hdfs://hadoop102:8020/flink/checkpoint"));
        //重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));
        //用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        return env;

    }


    public static StreamExecutionEnvironment getEnv(int port){
        StreamExecutionEnvironment env=getEnv(port, 4);
        return env;

    }
}
