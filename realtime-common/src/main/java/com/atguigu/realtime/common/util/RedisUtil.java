package com.atguigu.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.constant.Contant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Objects;

public class RedisUtil {
    public static void main(String[] args) {
        Jedis jedis=getJedis();

        StatefulRedisConnection<String, String> asyncConnection=getRedisAsyncConnection();


        JSONObject object=new JSONObject();
        object.put("name","aaa");
        putDataByAsync(asyncConnection,"test","222",object);
        System.out.println(Objects.requireNonNull(getDataByAsync(asyncConnection, "test", "222")).toJSONString());

        closeJedis(jedis);
    }

    private static JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig=new JedisPoolConfig();

        poolConfig.setMinIdle(5);
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "hadoop102", 6379, 10000);

    }

    //获取Jedis客户端连接
    public static Jedis getJedis(){
        Jedis jedis=jedisPool.getResource();
        return jedis;
    }

    //关闭Jedis客户端连接
    public static void closeJedis(Jedis jedis){
        if (jedis!=null) {
            jedis.close();
        }

    }

    //获取异步客户端连接
    public static StatefulRedisConnection<String,String> getRedisAsyncConnection(){

        RedisClient redisClient=RedisClient.create("redis://hadoop102:6379/0");

        StatefulRedisConnection<String, String> connect=redisClient.connect();
        return connect;

    }


    //关闭异步连接
    public static void closeRedisAsyncConnection(StatefulRedisConnection redisAsyncConnection){

        if(redisAsyncConnection != null && redisAsyncConnection.isOpen()){
            redisAsyncConnection.close();
        }

    }

    //从redis获取数据
    public static JSONObject getData(Jedis jedis, String tableName, String key){
        String data=jedis.get(getCombineKey(tableName, key));

        if (data != null){
            System.out.println("缓存命中");
            JSONObject jsonObject=JSON.parseObject(data);
            return jsonObject;
        }
        return null;
    }

    //向redis中插入数据
    public static void putData(Jedis jedis, String tableName, String key,JSONObject data ){
        jedis.setex(getCombineKey(tableName, key),24*60*60, data.toJSONString());

    }


    //以异步的方式从Redis中读取维度数据
    public static JSONObject getDataByAsync(StatefulRedisConnection<String, String> asyncRedisConns,String tableName,String key){
        //拼接查询Redis的key
        String combineKey = getCombineKey(tableName,key);
        //根据key到Redis中获取维度数据
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConns.async();

        try {
            String dimJsonStr = asyncCommands.get(combineKey).get();
            if(StringUtils.isNotEmpty(dimJsonStr)){
                //缓存命中
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }
    //以异步的方式向Redis中写入维度数据
    public static void putDataByAsync(StatefulRedisConnection<String, String> asyncRedisConns,String tableName,String key,JSONObject data){
        //拼接key
        String combineKey = getCombineKey(tableName, key);
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConns.async();
        asyncCommands.setex(combineKey,24*3600,data.toJSONString());
    }

    //获取Redis的RouKey
    private static String getCombineKey(String tableName, String key){
        return tableName + ":" + key;
    }


}
