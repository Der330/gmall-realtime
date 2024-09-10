package com.atguigu.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.constant.Contant;
import com.atguigu.realtime.common.util.HbaseUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import static com.atguigu.realtime.common.util.RedisUtil.*;
import static com.atguigu.realtime.common.util.RedisUtil.closeJedis;

public abstract class AddDimMapFunction<T> extends RichMapFunction<T,T> implements DimJoinFunction<T>{
    private Connection con;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis=getJedis();
        con=HbaseUtil.getHbaseConnect();
    }

    @Override
    public void close() throws Exception {
        closeJedis(jedis);
        HbaseUtil.closeHbaseConnect(con);
    }

    @Override
    public T map(T value) throws Exception {
        String rowKey = getRowKey(value);
        String tableName=getTableName();

        JSONObject dimData = getData(jedis, tableName, rowKey);

        if (dimData != null) {
        } else {
            dimData=HbaseUtil.getRow(con, Contant.HBASE_NAMESPACE, tableName, rowKey, JSONObject.class);
        }


        if (dimData != null) {
            putData(jedis, tableName, rowKey, dimData);
            addDim(value,dimData);
        } else {
            System.out.println("无DIM维度数据");
        }

        return value;
    }

}
