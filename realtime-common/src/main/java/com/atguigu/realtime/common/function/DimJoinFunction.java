package com.atguigu.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    String getTableName();

    String getRowKey(T value);

    void addDim(T value, JSONObject dimData);

}
