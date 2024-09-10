package com.atguigu.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T t) throws Exception {
        SerializeConfig config=new SerializeConfig();

        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);

        return JSON.toJSONString(t,config);
    }
}
