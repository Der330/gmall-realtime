package com.atguigu.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.HbaseUtil;
import com.atguigu.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.atguigu.realtime.common.util.RedisUtil.getDataByAsync;

public abstract class AddDimMapAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private AsyncConnection asyncHBaseConn;
    private StatefulRedisConnection<String, String> asyncRedisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncRedisConn=RedisUtil.getRedisAsyncConnection();
        asyncHBaseConn=HbaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(asyncRedisConn);
        HbaseUtil.closeHBaseAsyncConnection(asyncHBaseConn);
    }

    @Override
    public void asyncInvoke(T value, ResultFuture<T> resultFuture) throws Exception {

        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        JSONObject dimJSONObj=RedisUtil.getDataByAsync(asyncRedisConn, getTableName(), getRowKey(value));
                        return dimJSONObj;
                    }
                }
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJSONObj) {
                        if (dimJSONObj == null) {
                            try {
                                dimJSONObj = HbaseUtil.<JSONObject>getRowByAsync(asyncHBaseConn, Contant.HBASE_NAMESPACE, getTableName(), getRowKey(value), JSONObject.class);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        return dimJSONObj;
                    }
                }
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJSONObj) {
                        if (dimJSONObj != null) {

                            addDim(value, dimJSONObj);
                            RedisUtil.putDataByAsync(asyncRedisConn,getTableName(),getRowKey(value),dimJSONObj);
                        }
                        resultFuture.complete(Collections.singleton(value));

                    }
                }
        );
    }

}
