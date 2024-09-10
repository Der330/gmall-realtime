package com.atguigu.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TableProcessDim;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.HbaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;


public class DimApp {
    public static void main(String[] args) throws Exception {


        //环境
        //并行度
        //检查点
        //KafkaSource(db)
        //KafkaSource(rule)
        //根据维度表信息更新Hbase

        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", 10002);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //TODO 2.并行度
        env.setParallelism(4);
        //TODO 3.检查点
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

        //TODO 4.从kafka读取topic_db数据
        KafkaSource<String> kafkaSource=KafkaSource.<String>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setGroupId("dim_app")
                .setTopics(Contant.TOPIC_DB)
                .setStartingOffsets(OffsetsInitializer.latest())
                //使用默认的字符串反序列化器时，若数据为空，会报错，因此自己实现
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes != null) {
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        })
                .build();

        SingleOutputStreamOperator<JSONObject> dimDataDS=env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                                JSONObject jsonObject=JSONObject.parseObject(jsonStr);

                                String type=jsonObject.getString("type");
                                String data=jsonObject.getString("data");
                                String database=jsonObject.getString("database");
                                List<String> typeList=Arrays.asList("insert", "bootstrap-insert", "update", "delete");


                                if (
                                        "gmall".equals(database) &&
                                                typeList.contains(type) &&
                                                data != null &&
                                                data.length() > 2
                                ) {
                                    collector.collect(jsonObject);
                                }
                            }
                        }
                );


        //TODO 5.flinkCDC从mysql读取维度表信息

        Properties properties=new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource=MySqlSource.<String>builder()
                .hostname(Contant.MYSQL_HOST)
                .port(Contant.MYSQL_PORT)
                .jdbcProperties(properties)
                .username(Contant.MYSQL_USER_NAME)
                .password(Contant.MYSQL_PASSWORD)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        SingleOutputStreamOperator<TableProcessDim> dimRuleDS=env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "flinkCDC")
                .map(
                        new MapFunction<String, TableProcessDim>() {
                            @Override
                            public TableProcessDim map(String jsonStr) throws Exception {
                                JSONObject jsonObject=JSONObject.parseObject(jsonStr);

                                String op=jsonObject.getString("op");

                                TableProcessDim tableProcessDim=new TableProcessDim();
                                // r u c d
                                if ("d".equals(op)) {
                                    tableProcessDim=JSONObject.parseObject(jsonObject.getString("before"), TableProcessDim.class);
                                } else {
                                    tableProcessDim=JSONObject.parseObject(jsonObject.getString("after"), TableProcessDim.class);
                                }

                                tableProcessDim.setOp(op);
                                return tableProcessDim;
                            }
                        }
                )
                .map(
                        tableProcessDim -> {
                            Connection hbaseConnect=HbaseUtil.getHbaseConnect();
                            String op=tableProcessDim.getOp();
                            String sinkTable=tableProcessDim.getSinkTable();
                            String[] colFamilys=tableProcessDim.getSinkFamily().split(",");
                            if ("c".equals(op) || "r".equals(op)) {
                                HbaseUtil.createHbaseTable(hbaseConnect, Contant.HBASE_NAMESPACE, sinkTable, colFamilys);
                            }
                            if ("d".equals(op)) {
                                HbaseUtil.dropHbaseTable(hbaseConnect, Contant.HBASE_NAMESPACE, sinkTable);
                            }
                            if ("u".equals(op)) {
                                HbaseUtil.dropHbaseTable(hbaseConnect, Contant.HBASE_NAMESPACE, sinkTable);
                                HbaseUtil.createHbaseTable(hbaseConnect, Contant.HBASE_NAMESPACE, sinkTable, colFamilys);
                            }
                            return tableProcessDim;

                        }
                );


        MapStateDescriptor<String,TableProcessDim> dimTablesState=new MapStateDescriptor<String,TableProcessDim>(
                "dimTables",
                String.class,
                TableProcessDim.class
        );

        BroadcastStream<TableProcessDim> dimRuleBroadcastDS=dimRuleDS.broadcast(dimTablesState);

        //合并两条流，确定行为类型
        dimDataDS.connect(dimRuleBroadcastDS)
                .process(
                        new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                            Map<String,TableProcessDim> dimTableProcessMap=new HashMap<>();

                            //通过mysqlJDBC初始化dim表名集合
                            @Override
                            public void open(Configuration parameters) throws Exception {

                                //注册驱动
                                Class.forName(Contant.MYSQL_DRIVER);

                                //获得连接
                                java.sql.Connection mysqlConnect=DriverManager.getConnection(Contant.MYSQL_URL, Contant.MYSQL_USER_NAME, Contant.MYSQL_PASSWORD);

                                //获得数据库对象
                                String sql = "select * from gmall_config.table_process_dim";
                                PreparedStatement prepared=mysqlConnect.prepareStatement(sql);
                                //执行语句
                                ResultSet resultSet=prepared.executeQuery();

                                ResultSetMetaData metaData=resultSet.getMetaData();
                                //处理结果

                                while (resultSet.next()) {
                                    JSONObject jsonObject=new JSONObject();
                                    for (int i=1; i <= metaData.getColumnCount(); i++) {
                                        String calName=metaData.getCatalogName(i);
                                        Object colValue=resultSet.getObject(i);
                                        jsonObject.put(calName, colValue);
                                    }
                                    TableProcessDim tableProcessDim=JSONObject.parseObject(jsonObject.toJSONString(), TableProcessDim.class);
                                    dimTableProcessMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                                }
                                System.out.println("预加载维度表信息完成");

                                mysqlConnect.close();


                            }


                            @Override
                            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim,  Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext context, Collector< Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {

                                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState=context.getBroadcastState(dimTablesState);

                                String table=jsonObject.getString("table");


                                //判断是否为维度表的操作数据
                                if (broadcastState.contains(table) || dimTableProcessMap.containsKey(table)) {

                                    //maxwell生成的数据中data部分
                                    JSONObject data=JSONObject.parseObject(jsonObject.getString("data"));
                                    TableProcessDim tableProcessDim=broadcastState.get(table);

                                    //遍历维度表的规则，从maxwell数据中取到写入Hbase时需要的维度数据
                                    JSONObject resultObj=new JSONObject();
                                    String[] cols=tableProcessDim.getSinkColumns().split(",");
                                    for (String col : cols) {
                                        resultObj.put(col,data.getString(col));
                                    }

                                    String type=jsonObject.getString("type");
                                    resultObj.put("type",type);

                                    collector.collect(Tuple2.of(resultObj,tableProcessDim));

                                }
                            }

                            @Override
                            public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector< Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {

                                BroadcastState<String, TableProcessDim> broadcastState=context.getBroadcastState(dimTablesState);

                                String op=tp.getOp();
                                String sourceTable=tp.getSourceTable();

                                //更新dim广播状态的列表
                                if ("d".equals(op)) {
                                    //更新状态
                                    broadcastState.remove(sourceTable);
                                    dimTableProcessMap.remove(sourceTable);
                                } else {
                                    //更新状态
                                    broadcastState.put(sourceTable, tp);
                                    dimTableProcessMap.put(sourceTable, tp);
                                }

                            }
                        }
                )
                .addSink(
                        new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {

                            private Connection hbaseConnect;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseConnect=HbaseUtil.getHbaseConnect();
                            }

                            @Override
                            public void close() throws Exception {
                                hbaseConnect.close();
                            }

                            @Override
                            public void invoke(Tuple2<JSONObject, TableProcessDim> in, Context context) throws Exception {

                                JSONObject dataObj=in.f0;
                                TableProcessDim tableProcessDim=in.f1;

                                String type=dataObj.getString("type");
                                dataObj.remove("type");
                                String sinkTable=tableProcessDim.getSinkTable();
                                String sinkFamily=tableProcessDim.getSinkFamily();
                                String rowKey=dataObj.getString(tableProcessDim.getSinkRowKey());



                                if ("delete".equals(type)) {
                                    HbaseUtil.deleteRow(hbaseConnect,Contant.HBASE_NAMESPACE,sinkTable,rowKey);
                                } else {
                                    HbaseUtil.putRow(hbaseConnect,Contant.HBASE_NAMESPACE,sinkTable,sinkFamily,rowKey,dataObj);
                                }
                            }
                        }
                );

        //TODO 根据行为操作Hbase

        env.execute();



    }
}
