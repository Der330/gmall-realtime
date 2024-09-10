package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.Set;


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class WordSplitUDTF extends TableFunction<Row> {

    public void eval(String str) {
        Set<String> wordSet=IkUtil.splite(str);



        Iterator<String> iterator=wordSet.iterator();
        while (iterator.hasNext()) {
            String next=iterator.next();
            collect(Row.of(next));
        }
    }
}
