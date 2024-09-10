package com.atguigu.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.FlinkEnvUtil;
import com.atguigu.realtime.common.util.FlinkKafkaUtil;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.Date;

public class DwdBaseLog {
    public static void main(String[] args) throws Exception {

        //环境
        //并行度
        //检查点
        //KafkaSource(db)
        //KafkaSource(rule)
        //根据维度表信息更新Hbase

        //TODO 1.环境
        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10011, 4);


        //TODO 4.从kafka读取topic_log数据
        KafkaSource<String> kafkaSource=KafkaSource.<String>builder()
                .setBootstrapServers(Contant.KAFKA_BROKERS)
                .setGroupId("dwd_app")
                .setTopics(Contant.TOPIC_LOG)
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

        //清洗脏数据
        //修正is_new字段
        //识别日志种类
        //分流+封装
        //写入kafka
        OutputTag<String> dirty  = new OutputTag<String>("dirtyTag"){};
        OutputTag<String> err       = new OutputTag<String>("errTag") {};
        OutputTag<String> start     = new OutputTag<String>("startTag") {};
        OutputTag<String> action    = new OutputTag<String>("actionTag") {};
        OutputTag<String> page      = new OutputTag<String>("pageTag") {};
        OutputTag<String> display   = new OutputTag<String>("displayTag") {};


        DataStreamSource<String> ds=env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "logSource");

//        ds.print();
        SingleOutputStreamOperator<JSONObject> splitDS=ds
                //筛除格式错误流
                .process(

                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String in, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                                try {
                                    JSONObject jsonObject=JSON.parseObject(in);
                                    collector.collect(jsonObject);
                                } catch (Exception e) {
                                    //写入错误流中
                                    context.output(dirty, in);
                                }


                            }
                        }
                )
                //
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .process(

                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                            ValueState<String> lastDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor=new ValueStateDescriptor<String>("lastDateState", String.class);
                                lastDateState=getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {


                                //状态为null

                                JSONObject common=jsonObject.getJSONObject("common");
                                String isNew=common.getString("is_new");
                                String ts=jsonObject.getString("ts");
                                String tsDate=DateFormatUtils.format(Long.parseLong(ts), "yyyy-MM-dd");

                                //是否isNew
                                //是isNew
                                //状态为空：更新状态，isNew不变
                                //状态非空
                                //状态==ts：isNew不变
                                //状态！=ts：isNew改为0
                                //否isNew：更新状态为ts之前，isNew不变

                                if ("1".equals(isNew)) {


                                    String stateValue=lastDateState.value();
                                    if (stateValue == null) {
                                        lastDateState.update(tsDate);
                                    } else {
                                        if (stateValue != tsDate) {
                                            common.put("is_new", "0");
                                            jsonObject.put("common", common);
                                        }
                                    }
                                } else {
                                    Date date=DateUtils.addDays(new Date(Long.parseLong(ts)), -1);
                                    String dateStr=DateFormatUtils.format(date, "yyyy-MM-dd");
                                    lastDateState.update(dateStr);
                                }

                                collector.collect(jsonObject);


                            }
                        }
                )
                .process(
                        new ProcessFunction<JSONObject, JSONObject>() {
                            @Override
                            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {


                                //err
                                JSONObject errObject=jsonObject.getJSONObject("err");
                                if (errObject != null) {
                                    context.output(err, errObject.toJSONString());
                                    jsonObject.remove("err");
                                    System.out.println("错误日志:"+errObject.toJSONString());
                                }

                                JSONObject commonObject=jsonObject.getJSONObject("common");

                                //start
                                JSONObject startObject=jsonObject.getJSONObject("start");
                                if (startObject != null) {
                                    context.output(start, startObject.toJSONString());
                                    System.out.println("启动日志:"+startObject.toJSONString());
                                } else {


                                    JSONObject pageObject=jsonObject.getJSONObject("page");
                                    Long ts=Long.parseLong(jsonObject.getString("ts"));
                                    System.out.println(pageObject);

                                    if (pageObject != null) {

                                        //page
                                        JSONObject resultPageObj=new JSONObject();
                                        resultPageObj.put("page", pageObject);
                                        resultPageObj.put("ts", ts);
                                        resultPageObj.put("common", commonObject);

                                        context.output(page, resultPageObj.toJSONString());
                                        System.out.println("页面日志:"+resultPageObj.toJSONString());

                                        //display
                                        JSONArray displayArr=jsonObject.getJSONArray("displays");
                                        if (displayArr != null && displayArr.size() > 0) {
                                            for (int i=0; i < displayArr.size(); i++) {
                                                try {
                                                    JSONObject displayObject=displayArr.getJSONObject(i);
                                                    JSONObject resultDisplayObj=new JSONObject();
                                                    resultDisplayObj.put("common", commonObject);
                                                    resultDisplayObj.put("page", pageObject);
                                                    resultDisplayObj.put("ts", ts);
                                                    resultDisplayObj.put("display", displayObject);

                                                  context.output(display, resultDisplayObj.toJSONString());
                                                  System.out.println("曝光日志:"+resultDisplayObj.toJSONString());
                                                } catch (Exception e){
                                                    e.printStackTrace();
                                                }
                                            }
                                        }

                                        //action
                                        JSONArray actionArr=jsonObject.getJSONArray("actions");
                                        if (actionArr != null && actionArr.size() > 0) {
                                            for (int i=0; i < actionArr.size(); i++) {
                                                JSONObject actionObject=actionArr.getJSONObject(i);
                                                JSONObject resultActionObj=new JSONObject();
                                                resultActionObj.put("common", commonObject);
                                                resultActionObj.put("page", pageObject);
                                                resultActionObj.put("ts", ts);
                                                resultActionObj.put("display", actionObject);

                                                context.output(action, resultActionObj.toJSONString());
                                                System.out.println("行为日志:"+resultActionObj.toJSONString());
                                            }
                                        }
                                    }

                                }
                                collector.collect(jsonObject);

                            }
                        }
                );

        splitDS.print();
        splitDS.getSideOutput(err    ).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_err"));
        splitDS.getSideOutput(start  ).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_start"));
        splitDS.getSideOutput(action ).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_action"));
        splitDS.getSideOutput(page   ).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_page"));
        splitDS.getSideOutput(display).sinkTo(FlinkKafkaUtil.getKafkaSink("dwd_display"));

        env.execute();
    }
}
