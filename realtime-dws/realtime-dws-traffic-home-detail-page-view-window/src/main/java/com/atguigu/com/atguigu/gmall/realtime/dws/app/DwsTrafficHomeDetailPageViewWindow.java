package com.atguigu.com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.realtime.common.function.DorisMapFunction;
import com.atguigu.realtime.common.util.FlinkDorisUtil;
import com.atguigu.realtime.common.util.FlinkEnvUtil;
import com.atguigu.realtime.common.util.FlinkKafkaUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10023, 4);


        env.fromSource(FlinkKafkaUtil.getKafkaSource("dwd_page"), WatermarkStrategy.noWatermarks(),"dwd_page")
                .map(JSON::parseObject)

                .filter(
                        data -> "home".equals(data.getJSONObject("page").getString("page_id"))
                                ||"good_detail".equals(data.getJSONObject("page").getString("page_id"))
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                                return Long.parseLong(jsonObject.getString("ts"));
                                            }
                                        }
                                )
                )
                .keyBy( data -> data.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                            ValueState<String> homeStateDate;
                            ValueState<String> detailStateDate;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> homeStateDescriptor=new ValueStateDescriptor<String>("homeStateDate", String.class);
                                homeStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                homeStateDate=getRuntimeContext().getState(homeStateDescriptor);

                                ValueStateDescriptor<String> detailStateDescriptor=new ValueStateDescriptor<String>("detailStateDescriptor", String.class);
                                detailStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                detailStateDate =getRuntimeContext().getState(detailStateDescriptor);

                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {

                                JSONObject pageObj=jsonObject.getJSONObject("page");
                                String pageId=pageObj.getString("page_id");
                                Long ts=jsonObject.getLong("ts");

                                TrafficHomeDetailPageViewBean pageViewBean=new TrafficHomeDetailPageViewBean("", "", "", 0L, 0L, ts);

                                String viewDate=DateFormatUtils.format(ts, "yyyy-MM-dd");
                                String stateValue;
                                if ("home".equals(pageId)){
                                    stateValue = homeStateDate.value();
                                    if (stateValue == null) {
                                        homeStateDate.update(viewDate);
                                        pageViewBean.setHomeUvCt(1L);
                                    }else {
                                        if (!stateValue.equals(viewDate)) {
                                            pageViewBean.setHomeUvCt(1L);
                                        }
                                    }
                                }else {
                                    stateValue = detailStateDate.value();
                                    if (stateValue == null) {
                                        detailStateDate.update(viewDate);
                                        pageViewBean.setGoodDetailUvCt(1L);
                                    }else {
                                        if (!stateValue.equals(viewDate)) {
                                            pageViewBean.setGoodDetailUvCt(1L);
                                        }
                                    }
                                }

                                collector.collect(pageViewBean);


                            }
                        }
                )
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {
                                t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                                t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
//                                System.out.println(DateFormatUtils.format(t1.getTs(), "yyyy-MM-dd HH:mm:ss"));
                                return t1;
                            }
                        },
                        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                                TrafficHomeDetailPageViewBean pageViewBean=iterable.iterator().next();

                                String stt=DateFormatUtils.format(timeWindow.getStart(), "yyyy-MM-dd HH:mm:ss");
                                String edt=DateFormatUtils.format(timeWindow.getEnd(), "yyyy-MM-dd HH:mm:ss");
                                String curDate=DateFormatUtils.format(timeWindow.getStart(), "yyyy-MM-dd");

                                pageViewBean.setStt(stt);
                                pageViewBean.setEdt(edt);
                                pageViewBean.setCurDate(curDate);

                                collector.collect(pageViewBean);

                            }

                        }
                )
                .addSink(FlinkDorisUtil.jdbcSinkFunction("dws_traffic_home_detail_page_view_window", TrafficHomeDetailPageViewBean.class));


        env.execute();
    }
}
