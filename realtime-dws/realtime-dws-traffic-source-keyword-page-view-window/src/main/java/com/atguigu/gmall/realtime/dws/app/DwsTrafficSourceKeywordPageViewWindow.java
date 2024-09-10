package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.dws.function.WordSplitUDTF;
import com.atguigu.realtime.common.util.FlinkEnvUtil;
import com.atguigu.realtime.common.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) {
        StreamExecutionEnvironment env=FlinkEnvUtil.getEnv(10021, 4);

        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);

        tableEnv.createTemporaryFunction("split_word", WordSplitUDTF.class);

        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  `page` map<string,string>,\n" +
                "  `ts` bigint,\n" +
                "  `et` as to_timestamp_ltz(ts, 3),\n" +
                "  WATERMARK FOR et AS et \n" +
                ")" + SqlUtil.kafkaConnector("topic_log", "topic_log_traffic_source_keyword_page")
        );

        Table searchTable=tableEnv.sqlQuery("select\n" +
                "    page['item'] kw,\n" +
                "    et\n" +
                "from page_log\n" +
                "where page['item'] is not null\n" +
                "and page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'");

        tableEnv.createTemporaryView("search_table", searchTable);

        Table splitTable=tableEnv.sqlQuery("SELECT et, w\n" +
                "FROM search_table,\n" +
                "LATERAL TABLE(split_word(kw)) t(w)");

        tableEnv.createTemporaryView("split_table", splitTable);

        Table countTable=tableEnv.sqlQuery("SELECT\n" +
                "    date_format(window_start,'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    date_format(window_end,'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    date_format(window_start,'yyyy-MM-dd') cur_date,\n" +
                "    w keyword,\n" +
                "    count(*) keyword_count\n" +
                "FROM TABLE\n" +
                "(\n" +
                "  TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '5' SECOND)\n" +
                ")\n" +
                "GROUP BY window_start,window_end,w");

        tableEnv.createTemporaryView("count_table", countTable);
//        tableEnv.executeSql("select * from count_table").print();

        tableEnv.executeSql("create table dorisSink(\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                ")" + SqlUtil.dorisConnector("dws_traffic_source_keyword_page_view_window"));

        countTable.insertInto("dorisSink");

//        tableEnv.executeSql("\n" +
//                "INSERT INTO dorisSink (stt, edt, cur_date, keyword, keyword_count) VALUES\n" +
//                "('2024-09-04 12:30:45', '2024-09-04 13:30:45', '2024-09-04', 'example1', 123),\n" +
//                "('2024-09-04 14:45:30', '2024-09-04 15:45:30', '2024-09-04', 'example2', 901),\n" +
//                "('2024-09-04 16:00:00', '2024-09-04 17:00:00', '2024-09-04', 'example3', 012);\n");

    }
}
