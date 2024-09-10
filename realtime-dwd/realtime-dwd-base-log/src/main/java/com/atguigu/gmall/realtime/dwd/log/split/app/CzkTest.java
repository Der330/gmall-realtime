package com.atguigu.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.FlinkKafkaUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.Date;

public class CzkTest {
    public static void main(String[] args) throws Exception {

        //环境
        //并行度
        //检查点
        //KafkaSource(db)
        //KafkaSource(rule)
        //根据维度表信息更新Hbase

        //TODO 1.环境
        Configuration conf=new Configuration();
        conf.setInteger("rest.port", 8080);
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

        KafkaSource<String> kafkaSource=KafkaSource.<String>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setGroupId("Dwd_App")
                .setTopics(Contant.TOPIC_LOG)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes != null) {
                                    return new String(bytes);
                                } else {
                                    return null;
                                }
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                ).build();

        DataStreamSource<String> ds=env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "LogSource");

        OutputTag<String> errTag=new OutputTag<String>("errTag"){};
        OutputTag<String> pageTag=new OutputTag<String>("pageTag"){};
        OutputTag<String> displayTag=new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag=new OutputTag<String>("actionTag"){};
        OutputTag<String> startTag=new OutputTag<String>("startTag"){};


        //ETC
        SingleOutputStreamOperator<JSONObject> splitDS=ds.process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String jsonObjStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                try {
                                    JSONObject jsonObject=JSONObject.parseObject(jsonObjStr);
                                    collector.collect(jsonObject);
                                } catch (Exception e) {
                                    System.out.println("[错误字符串：]" + jsonObjStr);
                                }

                            }
                        }
                )
                //检查isNew字段
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .process(
                        new ProcessFunction<JSONObject, JSONObject>() {

                            ValueState<String> lastDate;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor=new ValueStateDescriptor<>("lastDate", String.class);
                                lastDate=getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                //isNew=1
                                //date！=null
                                //ts==date：不管
                                //ts！=date：修改isNew为0
                                //date==null：赋值date=ts
                                //isNew=0
                                //date==null:赋值date=ts-1d
                                String lastDateStr=lastDate.value();
                                JSONObject commonObj=jsonObject.getJSONObject("common");
                                String isNew=commonObj.getString("is_new");
                                long ts=Long.parseLong(jsonObject.getString("ts"));

                                if ("1".equals(isNew)) {
                                    String tsDate=DateFormatUtils.format(ts, "yyyy-MM-dd");
                                    if (lastDateStr == null) {
                                        lastDate.update(tsDate);
                                    } else {
                                        if (!lastDateStr.equals(tsDate)) {
                                            isNew="0";
                                            commonObj.put("is_new", isNew);
                                            jsonObject.put("common", commonObj);
                                        }
                                    }
                                } else {
                                    if (lastDate == null) {

                                        lastDateStr=DateFormatUtils.format(DateUtils.addDays(new Date(ts), -1), "yyyy-MM-dd");
                                        lastDate.update(lastDateStr);
                                    }
                                }

                                collector.collect(jsonObject);
                            }
                        }
                )
                //分流
                .process(
                        new ProcessFunction<JSONObject, JSONObject>() {
                            @Override
                            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                JSONObject commonObj=jsonObject.getJSONObject("common");
                                String ts=jsonObject.getString("ts");


                                //start
                                JSONObject startObj=jsonObject.getJSONObject("start");
                                if (startObj != null) {
                                    JSONObject startReObj=new JSONObject();
                                    startReObj.put("common", commonObj);
                                    startReObj.put("start", startObj);
                                    startReObj.put("ts", ts);

                                    context.output(startTag, startReObj.toJSONString());
                                    System.out.println("start" + startReObj.toJSONString());

                                }

                                //page
                                JSONObject pageObj=jsonObject.getJSONObject("page");
                                if (pageObj != null) {
                                    JSONObject pageReObj=new JSONObject();
                                    pageReObj.put("common", commonObj);
                                    pageReObj.put("page", pageObj);
                                    pageReObj.put("ts", ts);

                                    context.output(pageTag, pageReObj.toJSONString());

                                    System.out.println("page" + pageReObj.toJSONString());

                                    //display
                                    JSONArray displayArr=jsonObject.getJSONArray("displays");
                                    if (displayArr != null && displayArr.size() > 0) {
                                        for (int i=0; i < displayArr.size(); i++) {
                                            JSONObject displayObj=displayArr.getJSONObject(i);

                                            JSONObject displayReObj=new JSONObject();
                                            displayReObj.put("common", commonObj);
                                            displayReObj.put("page", pageObj);
                                            displayReObj.put("ts", ts);
                                            displayReObj.put("display", displayObj);

                                            context.output(displayTag, displayReObj.toJSONString());
                                            System.out.println("display" + displayReObj.toJSONString());
                                        }
                                    }

                                    //action
                                    JSONArray actionArr=jsonObject.getJSONArray("actions");
                                    if (actionArr != null && actionArr.size() > 0) {
                                        for (int i=0; i < actionArr.size(); i++) {
                                            JSONObject actionObj=actionArr.getJSONObject(i);

                                            JSONObject actionReObj=new JSONObject();
                                            actionReObj.put("common", commonObj);
                                            actionReObj.put("page", pageObj);
                                            actionReObj.put("ts", ts);
                                            actionReObj.put("action", actionObj);

                                            context.output(actionTag, actionReObj.toJSONString());
                                            System.out.println("action" + actionReObj.toJSONString());
                                        }
                                    }

                                }
                                collector.collect(jsonObject);
                            }
                        }
                );

        splitDS.getSideOutput(errTag).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_err"));
        splitDS.getSideOutput(pageTag).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_page"));
        splitDS.getSideOutput(displayTag).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_display"));
        splitDS.getSideOutput(actionTag).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_action"));
        splitDS.getSideOutput(startTag).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_start"));
        splitDS.getSideOutput(pageTag).print();

        env.execute();
    }
}
