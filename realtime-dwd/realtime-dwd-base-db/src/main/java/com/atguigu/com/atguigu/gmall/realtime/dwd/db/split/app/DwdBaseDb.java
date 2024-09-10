package com.atguigu.com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TableProcessDwd;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.FlinkEnvUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;


import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DwdBaseDb {
    public static void main(String[] args) throws Exception {
/*
1.获取kafka流，转换为JSONObject对象
2.获取mysql流、转换为对象
3.关联两条流，open初始化，用主流的table与侧流、map里的table比较，存在则处理，否则丢弃
4.自定义反序列化器，根据table指定不同的kafka主题

 */


        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10019, 4);

        //TODO 4.从kafka读取topic_db数据
        KafkaSource<String> kafkaSource=KafkaSource.<String>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setGroupId("dim_app")
                .setTopics(Contant.TOPIC_DB)
                .setStartingOffsets(OffsetsInitializer.latest())
                //使用默认的字符串反序列化器时，若数据为空，会报错，因此自己实现
                .setValueOnlyDeserializer(new SimpleStringSchema())
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
                .tableList("gmall_config.table_process_dwd")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>> myRecordSerializer=new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
            @Nullable
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> in, KafkaSinkContext kafkaSinkContext, Long aLong) {

                JSONObject jsonObject=in.f0;
                String topic=in.f1.getSinkTable();
                System.out.println(topic);
                return new ProducerRecord<>(topic, jsonObject.toJSONString().getBytes());
            }
        };
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink=KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setRecordSerializer(
                        myRecordSerializer

                ).build();



        SingleOutputStreamOperator<JSONObject> topicDS=env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "topic_db")
                .map(
                        new MapFunction<String, JSONObject>() {
                            @Override
                            public JSONObject map(String str) throws Exception {
                                try {
                                    JSONObject jsonObject=JSONObject.parseObject(str);
                                    return jsonObject;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }

                            }
                        }
                );



        SingleOutputStreamOperator<TableProcessDwd> mysqlSource=env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource")
                .map(
                        new MapFunction<String, TableProcessDwd>() {
                            @Override
                            public TableProcessDwd map(String jsonStr) throws Exception {
                                //为了处理方便，先将jsonStr转换为jsonObj
                                JSONObject jsonObj = JSON.parseObject(jsonStr);
                                //获取对配置表进行的操作的类型
                                String op = jsonObj.getString("op");
                                TableProcessDwd tableProcessDwd = null;
                                if ("d".equals(op)) {
                                    //从配置表中删除了一条数据  需要从before属性中获取删除前的配置
                                    tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                                } else {
                                    //从配置表中读取、向配置表中添加一条配置、对配置表信息进行了修改  都是从after属性中获取最终信息
                                    tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                                }
                                tableProcessDwd.setOp(op);
                                return tableProcessDwd;
                            }
                        }
                );

        MapStateDescriptor<String, TableProcessDwd> mapState=new MapStateDescriptor<>("table_process_dwd", String.class, TableProcessDwd.class);

        BroadcastStream<TableProcessDwd> broadcastStream=mysqlSource.broadcast(mapState);

        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDs=topicDS.connect(broadcastStream);

        connectDs.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>() {

                    public String getKey(String s1, String s2) throws Exception {
                        return s1+":"+s2;
                    }

                    Map<String,TableProcessDwd> tableProcessMap=new HashMap();
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        //注册驱动
                        Class.forName(Contant.MYSQL_DRIVER);

                        //获取连接
                        Connection connection=DriverManager.getConnection(Contant.MYSQL_URL,Contant.MYSQL_USER_NAME,Contant.MYSQL_PASSWORD);

                        //获得数据库对象
                        String sql = "select * from gmall_config.table_process_dwd";
                        PreparedStatement preparedStatement=connection.prepareStatement(sql);

                        //运行语句
                        ResultSet resultSet=preparedStatement.executeQuery();

                        ResultSetMetaData metaData=resultSet.getMetaData();

                        //处理结果
                        while (resultSet.next()) {
                            JSONObject jsonObject=new JSONObject();
                            for (int i=1; i <= metaData.getColumnCount(); i++) {
                                String catalogName=metaData.getCatalogName(i);
                                Object catalogValue=resultSet.getObject(i);
                                jsonObject.put(catalogName,catalogValue);
                            }
                            TableProcessDwd tableProcessDwd=JSONObject.parseObject(jsonObject.toJSONString(), TableProcessDwd.class);

                            String key=getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
                            tableProcessMap.put(key, tableProcessDwd);
                        }
                        System.out.println("预加载完成");

                        //关闭连接
                        connection.close();
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd,  Tuple2<JSONObject,TableProcessDwd>>.ReadOnlyContext context, Collector< Tuple2<JSONObject,TableProcessDwd>> collector) throws Exception {

                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState=context.getBroadcastState(mapState);

                        String key=getKey(jsonObject.getString("table"), jsonObject.getString("type"));
                        TableProcessDwd tableProcessDwd;
                        if ( (tableProcessDwd = broadcastState.get(key))!= null
                           ||(tableProcessDwd = tableProcessMap.get(key))!= null      ) {

                            String[] columnList=tableProcessDwd.getSinkColumns().split(",");

                            JSONObject resultObj=new JSONObject();
                            resultObj.put("type",jsonObject.getString("type"));
                            resultObj.put("ts",jsonObject.getString("ts"));
                            //挑出需要的字段
                            JSONObject data=jsonObject.getJSONObject("data");
                            for (String column : columnList) {
                                resultObj.put(column,data.getString(column));
                            }

                            collector.collect(Tuple2.of(resultObj,tableProcessDwd));

                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd,  Tuple2<JSONObject,TableProcessDwd>>.Context context, Collector< Tuple2<JSONObject,TableProcessDwd>> collector) throws Exception {

                        BroadcastState<String, TableProcessDwd> broadcastState=context.getBroadcastState(mapState);

                        String op=tableProcessDwd.getOp();
                        String sourceTable=tableProcessDwd.getSourceTable();

                        String key=getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());

                        if ("d".equals(op)) {
                            broadcastState.remove(sourceTable);
                            tableProcessMap.remove(sourceTable);
                        } else {
                            broadcastState.put(key, tableProcessDwd);
                            tableProcessMap.put(key,tableProcessDwd);
                        }

                    }
                }
        )
                .sinkTo(kafkaSink);


        env.execute();



    }
}
