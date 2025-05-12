package com.ytx.realtime.dwd;

import com.ytx.common.base.BaseSQLApp;
import com.ytx.common.constant.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RealtimeAgeTagCalculator extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new RealtimeAgeTagCalculator().start(10030,4,Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {

        tableEnv.executeSql("CREATE TABLE topic_db_yue (\n" +
                "  `op` string,\n" +
                "  `before` MAP<string,string>,\n" +
                " `after` MAP<string,string>,\n" +
                "  `source` MAP<string,string>,\n" +
                " `ts_ms` bigint,\n" +
                "`pt` as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'tianxin_yueyw',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'my_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db_yue").print();


        tableEnv.executeSql("CREATE TABLE topic_log_yue (\n" +
                "  `common` MAP<string,string>,\n" +
                " `page` MAP<string,string>,\n" +
                " `ts` bigint,\n" +
                "`pt` as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'tianxin_yue_dwd_traffic_page',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'my_group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_log_yue").print();
//        用户表
        Table userinfo = tableEnv.sqlQuery("select\n" +
                "             `after`['id'] id,\n" +
                "             `after`['name'] name ,\n" +
                "             `after`['phone_num'] phone_num ,\n" +
                "             `after`['email'] email ,\n" +
                "             `after`['user_level'] user_level ,\n" +
                "   after['birthday'] birthday, " +
                "              `after`['gender'] gender,\n" +
                "              `after`['create_time'] create_time,\n" +
                "              `after`['operate_time'] operate_time,\n" +
                "              `after`['status'] status,\n" +
                "               `ts_ms`,\n" +
                "                `pt`\n" +
                "from topic_db_yue where source['table']='user_info'");
       userinfo.execute().print();
        tableEnv.createTemporaryView("user_info", userinfo);
//        一级品类
        Table basecategory1 = tableEnv.sqlQuery("select\n" +
                "             `after`['id'] id,\n" +
                "             `after`['name'] name ,\n" +
                "              `after`['create_time'] create_time,\n" +
                "              `after`['operate_time'] operate_time,\n" +
                "               `ts_ms`,\n" +
                "                `pt`\n" +
                "from topic_db_yue where source['table']='base_category1'");
//        basecategory1.execute().print();
        tableEnv.createTemporaryView("base_category1", basecategory1);
        //        二级品类
        Table basecategory2 = tableEnv.sqlQuery("select\n" +
                "             `after`['id'] id,\n" +
                "             `after`['name'] name ,\n" +
                "             `after`['category1_id'] name ,\n" +
                "              `after`['create_time'] create_time,\n" +
                "              `after`['operate_time'] operate_time,\n" +
                "               `ts_ms`,\n" +
                "                `pt`\n" +
                "from topic_db_yue where source['table']='base_category2'");
//        basecategory2.execute().print();
        tableEnv.createTemporaryView("base_category2", basecategory2);
        Table basecategory3 = tableEnv.sqlQuery("select\n" +
                "             `after`['id'] id,\n" +
                "             `after`['name'] name ,\n" +
                "             `after`['category2_id'] name ,\n" +
                "              `after`['create_time'] create_time,\n" +
                "              `after`['operate_time'] operate_time,\n" +
                "               `ts_ms`,\n" +
                "                `pt`\n" +
                "from topic_db_yue where source['table']='base_category3'");
//       basecategory3.execute().print();
        tableEnv.createTemporaryView("base_category3", basecategory3);
        Table basetrademark = tableEnv.sqlQuery("select\n" +
                "             `after`['id'] id,\n" +
                "             `after`['tm_name'] name ,\n" +
                "             `after`['logo_url'] name ,\n" +
                "              `after`['create_time'] create_time,\n" +
                "              `after`['operate_time'] operate_time,\n" +
                "               `ts_ms`,\n" +
                "                `pt`\n" +
                "from topic_db_yue where source['table']='base_trademark'");
//       basetrademark.execute().print();
        tableEnv.createTemporaryView("basetrademark", basetrademark);
        Table orderDetail = tableEnv.sqlQuery("select\n" +
                " `after`['id'] id,\n" +
                " `after`['order_id'] order_id,\n" +
                " `after`['sku_id'] sku_id,\n" +
                " `after`['sku_name'] sku_name,\n" +
                " `after`['create_time'] create_time,\n" +
                " `after`['source_id'] source_id,\n" +
                " `after`['source_type'] source_type,\n" +
                " `after`['sku_num'] sku_num,\n" +
                "cast(cast(`after`['sku_num'] as decimal(16,2))*\n" +
                "cast(`after`['order_price'] as decimal(16,2)) as string) split_original_amount,\n" +
                " `after`['split_total_amount'] split_total_amount,\n" +
                " `after`['split_activity_amount'] split_activity_amount,\n" +
                " `after`['split_coupon_amount'] split_coupon_amount,\n" +
                "ts_ms\n" +
                "from topic_db_yue where `source`['table']='order_detail'\n" );
//        orderDetail.execute().print();
        tableEnv.createTemporaryView("order_detail",orderDetail);


    }


}
