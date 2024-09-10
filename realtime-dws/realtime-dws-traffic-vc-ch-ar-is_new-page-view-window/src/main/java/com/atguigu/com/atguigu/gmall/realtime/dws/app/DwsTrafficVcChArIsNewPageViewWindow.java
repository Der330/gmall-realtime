package com.atguigu.com.atguigu.gmall.realtime.dws.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TrafficPageViewBean;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10022, 4);

        SingleOutputStreamOperator<String> mapDS=env.fromSource(FlinkKafkaUtil.getKafkaSource("dwd_page"), WatermarkStrategy.noWatermarks(), "dwdPageSource")
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {

                            ValueState<String> lastDateState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> lastLoginDate=new ValueStateDescriptor<String>("lastLoginDate", String.class);
                                lastLoginDate.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1)).build());

                                lastDateState=getRuntimeContext().getState(lastLoginDate);


                            }

                            @Override
                            public void processElement(JSONObject jsonOb, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {

                                JSONObject commonObj=jsonOb.getJSONObject("common");
                                JSONObject pageObj=jsonOb.getJSONObject("page");

                                String vc=commonObj.getString("vc");
                                String ch=commonObj.getString("ch");
                                String ar=commonObj.getString("ar");
                                String isNew=commonObj.getString("is_new");

                                String sid=commonObj.getString("sid");
                                long duringTime=Long.parseLong(pageObj.getString("during_time"));
                                long ts=Long.parseLong(jsonOb.getString("ts"));
                                String nowDate=DateFormatUtils.format(ts, "yyyy-MM-dd");
                                String lastPageId=pageObj.getString("last_page_id");


                                Long uvCt=0L;
                                String stateDate=lastDateState.value();
                                if (stateDate == null) {
                                    lastDateState.update(nowDate);
                                    uvCt=1L;
                                } else {
                                    if (nowDate.equals(stateDate)) {
                                        uvCt=1L;
                                    }
                                }

                                Long svCt=0L;
                                if ("null".equals(lastPageId) || lastPageId == null) {
                                    svCt=1L;
                                }

                                TrafficPageViewBean pageViewBean=new TrafficPageViewBean(
                                        "",
                                        "",
                                        "",
                                        vc,
                                        ch,
                                        ar,
                                        isNew,
                                        uvCt,
                                        svCt,
                                        1L,
                                        duringTime,
                                        ts,
                                        sid
                                );

                                collector.collect(pageViewBean);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps()
                                .withTimestampAssigner((t, ts) -> t.getTs())
                )
                .keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean t) throws Exception {
                        return Tuple4.of(t.getVc(), t.getCh(), t.getAr(), t.getIsNew());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                                t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                                t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                                t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                                t1.setDurSum(t1.getDurSum() + t2.getDurSum());
//                                                System.out.println(DateFormatUtils.format(t1.getTs(),"yyyy-MM-dd HH:mm:ss"));
                                return t1;
                            }
                        },
                        new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                                TrafficPageViewBean pageViewBean=iterable.iterator().next();
                                String cur_date=DateFormatUtils.format(timeWindow.getStart(), "yyyy-MM-dd");
                                String stt=DateFormatUtils.format(timeWindow.getStart(), "yyyy-MM-dd HH:mm:ss");
                                String edt=DateFormatUtils.format(timeWindow.getEnd(), "yyyy-MM-dd HH:mm:ss");

                                pageViewBean.setStt(stt);
                                pageViewBean.setEdt(edt);
                                pageViewBean.setCur_date(cur_date);

                                collector.collect(pageViewBean);

                            }
                        }
                )
                .map(new DorisMapFunction<>());



        mapDS.sinkTo(FlinkDorisUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
        mapDS.print();

//        env.fromElements("{\"ar\":\"18\",\"ch\":\"360\",\"cur_date\":\"2024-09-03\",\"dur_sum\":61814,\"edt\":\"2024-09-03 11:15:30\",\"is_new\":\"0\",\"pv_ct\":5,\"stt\":\"2024-09-03 11:15:20\",\"sv_ct\":2,\"uv_ct\":5,\"vc\":\"v2.1.134\"}")
//                        .sinkTo(FlinkDorisUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));



        env.execute();


    }
}
