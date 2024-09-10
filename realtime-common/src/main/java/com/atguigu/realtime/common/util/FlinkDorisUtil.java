package com.atguigu.realtime.common.util;

import com.alibaba.fastjson.annotation.JSONField;
import com.atguigu.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.realtime.common.constant.Contant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkDorisUtil {
    public static DorisSink<String> getDorisSink(String tableName){

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Contant.DORIS_FE_NODES)
                        .setTableIdentifier(Contant.DORIS_DATABASE + "." + tableName)
                        .setUsername("root")
                        .setPassword(Contant.DORIS_PASSWORD)
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }



    public static <T> SinkFunction<T> jdbcSinkFunction(String tableName, Class<T> clz) throws SQLException {

        Field[] fields = clz.getDeclaredFields();
        int colNum =fields.length;

        ArrayList<Tuple2<String, String>> colTypeList=new ArrayList<>();
        //获得要插入字段的数量
        for (Field field : fields) {
            //获得需要插入表格的变量名和类型
            colTypeList.add(Tuple2.of(field.getName(),field.getType().getName()));

            if (field.isAnnotationPresent(JSONField.class)){
                JSONField jsonField=field.getAnnotation(JSONField.class);
                if (!jsonField.serialize()) {
                    colNum = colNum-1;

                }

            }
        }

        //删除非Doris表字段
        while (colTypeList.size() != colNum){
            colTypeList.remove(colTypeList.size()-1);
        }

        String sql=getInsertSql(tableName, colNum);

        SinkFunction<T> dorisSinkFunctionInJdbc=JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T data) throws SQLException {

                        //向插入语句中填充数据
                        insertData(ps, data, colTypeList);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop107:9030/gmall_realtime")//127.0.0.1:12000
                        .withUsername("root")
                        .withPassword(Contant.DORIS_PASSWORD)
                        .build()

        );

        return dorisSinkFunctionInJdbc;
    }

    //获得插入SQL语句
    private static String getInsertSql(String tableName, int colNum){
        //生成sql插入语句占位符
        StringBuffer value=new StringBuffer();
        for (int i=1; i <= colNum; i++) {
            if (i != colNum){
                value.append("?,");
            }else {
                value.append("?");
            }
        }
        String sql = "INSERT INTO " + tableName + " VALUES ( "+value.toString()+" )";

        return sql;
    }

    private static <T> void insertData(PreparedStatement ps, T data, List<Tuple2<String,String>> colTypeList) throws SQLException {

        System.out.println("Size:"+colTypeList.size());
        System.out.println(colTypeList);
        //向插入语句中填充数据
        for (int i=0; i < colTypeList.size(); i++) {
            int index = i+1;

            Tuple2<String, String> colTypeObj=colTypeList.get(i);
            System.out.println("变量： "+colTypeObj);
            String colName=colTypeObj.f0;
            String colType=colTypeObj.f1;


            //获取对应属性的值
            Object value = null;
            try {
                Field field=data.getClass().getDeclaredField(colName);
                field.setAccessible(true);
                value = field.get(data);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }


            if (colType.contains("String")) {
                System.out.println("String"+"\t"+value);
                ps.setString(index, (String) value);
            }
            if (colType.contains("Long")) {
                System.out.println("Long"+"\t"+value);
                ps.setLong(index, (Long) value);
            }
            if (colType.contains("BigDecimal")) {
                System.out.println("BigDecimal"+"\t"+value);
                try {
                    ps.setBigDecimal(index, (BigDecimal) value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
