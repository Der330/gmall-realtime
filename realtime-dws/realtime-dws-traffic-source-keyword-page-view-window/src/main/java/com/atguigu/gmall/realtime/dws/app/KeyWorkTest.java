package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.dws.function.WordSplitUDTF;
import com.atguigu.realtime.common.util.FlinkEnvUtil;
import com.atguigu.realtime.common.util.FlinkKafkaUtil;
import com.atguigu.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeyWorkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(20231, 4);

        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);

        tableEnv.createTemporaryFunction("split_word", WordSplitUDTF.class);

        tableEnv.executeSql("CREATE TABLE dwd_page (\n" +
                "  `page` map<string,string>,\n" +
                "  `ts` bigint,\n" +
                "  `et` as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR et AS et\n" +
                ")"+ SqlUtil.kafkaConnector("dwd_page","dwd_page"));

        Table pageItem=tableEnv.sqlQuery("select\n" +
                "    page['item'] kw,\n" +
                "    et\n" +
                "from dwd_page\n" +
                "where page['item'] is not null\n" +
                "and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");

        tableEnv.createTemporaryView("pageItem",pageItem);


        Table splitedTable=tableEnv.sqlQuery("select\n" +
                "    et,\n" +
                "    word\n" +
                "from pageItem,\n" +
                "LATERAL TABLE(split_word(kw)) t(word)");

        tableEnv.createTemporaryView("splitedTable",splitedTable);

        Table resultTable=tableEnv.sqlQuery("SELECT\n" +
                "    date_format(window_start,'yyyy-MM-dd HH:mm:ss') as stt,\n" +
                "    date_format(window_end,'yyyy-MM-dd HH:mm:ss') as edt,\n" +
                "    date_format(window_start,'yyyy-MM-dd') as cur_date,\n" +
                "    word keyword,\n" +
                "    count(*) AS keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE splitedTable, DESCRIPTOR(et), INTERVAL '10' SECOND))\n" +
                "  GROUP BY window_start,window_end,word");

        tableEnv.createTemporaryView("resultTable",resultTable);
        tableEnv.executeSql("select * from resultTable").print();


        TableResult dorisSink=tableEnv.executeSql("CREATE TABLE dorisSink (\n" +
                "    stt           string,\n" +
                "    edt           string,\n" +
                "    cur_date     string,\n" +
                "    keyword       string,\n" +
                "    keyword_count BIGINT\n" +
                "    )" + SqlUtil.dorisConnector("dws_traffic_source_keyword_page_view_window"));

        resultTable.insertInto("dorisSink");


    }
}
