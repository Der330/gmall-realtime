package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.function.AddDimMapAsyncFunction;
import com.atguigu.realtime.common.function.AddDimMapFunction;
import com.atguigu.realtime.common.util.*;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.atguigu.realtime.common.util.RedisUtil.*;
import static org.apache.hadoop.hbase.util.CommonFSUtils.getTableName;

public class DwsTradeSkuOrderWindowSyncCache {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10029, 4);

        DataStreamSource<String> sourceDs=env.fromSource(FlinkKafkaUtil.getKafkaSource(Contant.TOPIC_DWD_TRADE_ORDER_DETAIL), WatermarkStrategy.noWatermarks(), "source");

        SingleOutputStreamOperator<String> filterDS=sourceDs.filter(data -> data != null);

        SingleOutputStreamOperator<JSONObject> mapDS=filterDS.map(JSON::parseObject);

        KeyedStream<JSONObject, String> keyByDS=mapDS.keyBy(jsonObj -> jsonObj.getString("id"));

        SingleOutputStreamOperator<TradeSkuOrderBean> noRepeatDS=keyByDS.process(
                new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {

                    private TradeSkuOrderBean jsonObjToBean(JSONObject jsonObj) {
                        String orderDetailId=jsonObj.getString("id");
                        String skuId=jsonObj.getString("sku_id");
                        BigDecimal originalAmount=jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal activityAmount=jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal couponAmount=jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal totalAmount=jsonObj.getBigDecimal("split_total_amount");
                        Long ts=jsonObj.getLong("ts")*1000L;

                        TradeSkuOrderBean skuOrderBean=TradeSkuOrderBean.builder()
                                .orderDetailId(orderDetailId)
                                .skuId(skuId)
                                .originalAmount(originalAmount)
                                .activityReduceAmount(activityAmount)
                                .couponReduceAmount(couponAmount)
                                .orderAmount(totalAmount)
                                .ts(ts).build();

                        return skuOrderBean;
                    }

                    ValueState<JSONObject> valueState;

                    @Override
                    public void open(Configuration ps) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor=new ValueStateDescriptor<>("valueState", JSONObject.class);

                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5)).build());

                        valueState=getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {

                        //状态+抵消

                        JSONObject stateJSONObj=valueState.value();

                        if (stateJSONObj == null) {
                            valueState.update(jsonObject);
                            collector.collect(jsonObjToBean(jsonObject));
                        } else {

                            stateJSONObj.put("split_original_amount", "-" + stateJSONObj.getString("split_original_amount"));
                            stateJSONObj.put("split_activity_amount", "-" + stateJSONObj.getString("split_activity_amount"));
                            stateJSONObj.put("split_coupon_amount", "-" + stateJSONObj.getString("split_coupon_amount"));
                            stateJSONObj.put("split_total_amount", "-" + stateJSONObj.getString("split_total_amount"));

                            valueState.clear();
                            collector.collect(jsonObjToBean(stateJSONObj));
                            collector.collect(jsonObjToBean(jsonObject));

                        }
                    }
                }
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> watermarkedDS=noRepeatDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeSkuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //
        SingleOutputStreamOperator<TradeSkuOrderBean> winReducedDS=watermarkedDS.keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {

                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                return value1;
                            }
                        },
                        new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void apply(String key, TimeWindow timeWindow, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                                TradeSkuOrderBean skuOrderBean=iterable.iterator().next();

                                String stt=DateFormatUtils.format(timeWindow.getStart(), "yyyy-MM-dd HH:mm:ss");
                                String edt=DateFormatUtils.format(timeWindow.getEnd(), "yyyy-MM-dd HH:mm:ss");
                                String curDate=DateFormatUtils.format(timeWindow.getStart(), "yyyy-MM-dd");

                                skuOrderBean.setStt(stt);
                                skuOrderBean.setEdt(edt);
                                skuOrderBean.setCurDate(curDate);

                                collector.collect(skuOrderBean);
                            }
                        }
                );


        //旁路缓存+模板方法
        SingleOutputStreamOperator<TradeSkuOrderBean> addSkuDimDS=winReducedDS.map(
                new AddDimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean value) {
                        return value.getSkuId();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
                        value.setSkuName(dimData.getString("sku_name"));
                        value.setSpuId(dimData.getString("spu_id"));
                        value.setTrademarkId(dimData.getString("tm_id"));
                        value.setCategory3Id(dimData.getString("category3_id"));
                    }
                }
        );

        //旁路缓存 + 异步连接
//        SingleOutputStreamOperator<TradeSkuOrderBean> addSpuDimDS=AsyncDataStream.unorderedWait(
//                addSkuDimDS,
//                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//                    private AsyncConnection asyncHBaseConn;
//                    private StatefulRedisConnection<String, String> asyncRedisConn;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        asyncRedisConn=RedisUtil.getRedisAsyncConnection();
//                        asyncHBaseConn=HbaseUtil.getHBaseAsyncConnection();
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        RedisUtil.closeRedisAsyncConnection(asyncRedisConn);
//                        HbaseUtil.closeHBaseAsyncConnection(asyncHBaseConn);
//                    }
//                    @Override
//                    public void asyncInvoke(TradeSkuOrderBean tradeSkuOrderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
//                        CompletableFuture.supplyAsync(
//                                new Supplier<JSONObject>() {
//                                    @Override
//                                    public JSONObject get() {
//                                        JSONObject dimJSONObj=getDataByAsync(asyncRedisConn, "dim_spu_info", tradeSkuOrderBean.getSpuId());
//                                        return dimJSONObj;
//                                    }
//                                }
//                        ).thenApplyAsync(
//                                new Function<JSONObject, JSONObject>() {
//                                    @Override
//                                    public JSONObject apply(JSONObject dimJSONObj) {
//                                        if (dimJSONObj == null) {
//                                            try {
//                                                dimJSONObj = HbaseUtil.<JSONObject>getRowByAsync(asyncHBaseConn, Contant.HBASE_NAMESPACE, "dim_spu_info", tradeSkuOrderBean.getSpuId(), JSONObject.class);
//                                            } catch (Exception e) {
//                                                throw new RuntimeException(e);
//                                            }
//                                        }
//
//                                        return dimJSONObj;
//                                    }
//                                }
//                        ).thenAcceptAsync(
//                                new Consumer<JSONObject>() {
//                                    @Override
//                                    public void accept(JSONObject dimJSONObj) {
//                                        if (dimJSONObj != null) {
//
//                                            tradeSkuOrderBean.setSpuName(dimJSONObj.getString("spu_name"));
//                                            RedisUtil.putDataByAsync(asyncRedisConn,"dwd_spu_info",tradeSkuOrderBean.getSpuId(),dimJSONObj);
//                                        }
//                                        resultFuture.complete(Collections.singleton(tradeSkuOrderBean));
//
//                                    }
//                                }
//                        );
//                    }
//                },
//                60,
//                TimeUnit.SECONDS);

//        SingleOutputStreamOperator<TradeSkuOrderBean> addSpuDimDS1=addSkuDimDS.map(
//                new AddDimMapFunction<TradeSkuOrderBean>() {
//                    @Override
//                    public String getTableName() {
//                        return "dim_spu_info";
//                    }
//
//                    @Override
//                    public String getRowKey(TradeSkuOrderBean value) {
//                        return value.getSpuId();
//                    }
//
//                    @Override
//                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
//                        value.setSpuName(dimData.getString("spu_name"));
//                    }
//                }
//        );


        SingleOutputStreamOperator<TradeSkuOrderBean> addSpuDimDS=AsyncDataStream.unorderedWait(
                addSkuDimDS,
                new AddDimMapAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean value) {
                        return value.getSpuId();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
                        value.setSpuName(dimData.getString("spu_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> addTmDimDS=AsyncDataStream.unorderedWait(
                addSpuDimDS,
                new AddDimMapAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean value) {
                        return value.getTrademarkId();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
                        value.setTrademarkName(dimData.getString("tm_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> addCate3DimDS=AsyncDataStream.unorderedWait(
                addTmDimDS,
                new AddDimMapAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean value) {
                        return value.getCategory3Id();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
                        value.setCategory3Name(dimData.getString("name"));
                        value.setCategory2Id(dimData.getString("category2_id"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> addCate2DimDS=AsyncDataStream.unorderedWait(
                addCate3DimDS,
                new AddDimMapAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean value) {
                        return value.getCategory2Id();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
                        value.setCategory2Name(dimData.getString("name"));
                        value.setCategory1Id(dimData.getString("category1_id"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> addCate1DimDS=AsyncDataStream.unorderedWait(
                addCate2DimDS,
                new AddDimMapAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean value) {
                        return value.getCategory1Id();
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean value, JSONObject dimData) {
                        value.setCategory1Name(dimData.getString("name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        addCate1DimDS.print();

        addCate1DimDS.addSink(FlinkDorisUtil.jdbcSinkFunction("dws_trade_sku_order_window", TradeSkuOrderBean.class));





        env.execute();


    }
}
