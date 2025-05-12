package com.ytx.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytx.common.base.BaseApp;
import com.ytx.common.base.BaseSQLApp;
import com.ytx.common.constant.Constant;
import com.ytx.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class dwduserinfo extends BaseApp implements Serializable {
    public static void main(String[] args) throws Exception {
        new dwduserinfo().start(
                10029,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (s != null) {
                    JSONObject jsonObj = JSON.parseObject(s);
                    out.collect(jsonObj);
                }
            }
        });
//        jsonObjDs.print();
        SingleOutputStreamOperator<JSONObject> filtered = jsonObjDs.filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("user_info"));
//        filtered.print();


        SingleOutputStreamOperator<JSONObject> userInfoStream = filtered.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject afterobj = jsonObject.getJSONObject("after");
                // 如果 afterObj 不为空且包含 birthday 字段
                if (afterobj != null && afterobj.containsKey("birthday")) {
                    Object birthdayValue = afterobj.get("birthday");

                    if (birthdayValue != null) {
                        String daySinceBaseStr = birthdayValue.toString();
                        try {
                            long days = Long.parseLong(daySinceBaseStr);
                            LocalDate baselDate = LocalDate.of(1970, 1, 1);
                            // 计算实际出生日期
                            LocalDate actualBirthday = baselDate.plusDays(days);
                            // 使用 ISO 标准格式化日期（如：2023-04-05）
                            String formatDate = actualBirthday.format(DateTimeFormatter.ISO_LOCAL_DATE);
                            afterobj.put("birthday", formatDate);
                            // 计算年龄并添加年龄段
                            int age = Period.between(actualBirthday, LocalDate.now()).getYears();
                            afterobj.put("age", age);
                            afterobj.put("age_group", calculateAgeGroup(age));

                        } catch (NumberFormatException e) {
                            afterobj.put("birthday", "invalid_date");
                            afterobj.put("age", -1);
                            afterobj.put("age_group", "unknown");
                        }
                    }
                }
                return jsonObject;
            }
        });
    userInfoStream.print();
//一级品类
        SingleOutputStreamOperator<JSONObject> basecategory1 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("base_category1"));
//        二级品类
        SingleOutputStreamOperator<JSONObject> basecategory2 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("base_category2"));
//        三级品类
        SingleOutputStreamOperator<JSONObject> basecategory3 = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("base_category3"));
//    basecategory3.print();
//     品牌表
        SingleOutputStreamOperator<JSONObject> baseTrademark = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("base_trademark"));
//        评论表
        SingleOutputStreamOperator<JSONObject> baseCommentInfo = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("comment_info"));
//        订单明细
        SingleOutputStreamOperator<JSONObject> order_detail = kafkaSource.map(new RichMapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).filter(jsonObj -> jsonObj.getJSONObject("source").getString("table").equals("order_detail"));

        tableEnv.createTemporaryView("user_info", userInfoStream);
        tableEnv.createTemporaryView("base_category1", basecategory1);
        tableEnv.createTemporaryView("base_category2", basecategory2);
        tableEnv.createTemporaryView("base_category3", basecategory3);
        tableEnv.createTemporaryView("base_trademark", baseTrademark);
        tableEnv.createTemporaryView("comment_info", baseCommentInfo);
        tableEnv.createTemporaryView("order_detail", order_detail);



        // 4. 执行关联查询
//        Table result = tableEnv.sqlQuery(
//                "SELECT " +
//                        "  u.after['id'] AS user_id, " +
//                        "  u.after['name'] AS user_name, " +
//                        "  u.after['phone_num'] AS phone, " +
//                        "  u.after['email'] AS email, " +
//                        "  u.after['birthday'] AS birthday, " +
//                        "  u.after['age'] AS age, " +
//                        "  u.after['age_group'] AS age_group, " +
//                        "  o.after['order_id'] AS last_order_id, " +
//                        "  c.after['comment_text'] AS last_comment, " +
//                        "  t.after['tm_name'] AS favorite_brand " +
//                        "FROM user_info u " +
//                        "LEFT JOIN order_detail o ON u.after['id'] = o.after['user_id'] " +
//                        "LEFT JOIN comment_info c ON u.after['id'] = c.after['user_id'] " +
//                        "LEFT JOIN base_trademark t ON u.after['favorite_brand_id'] = t.after['id']"
//        );
////
////        // 5. 输出结果
//        result.execute().print();
    }
    private String calculateAgeGroup(int age) {
        if (age < 18) return "Under 18";
        else if (age <= 24) return "18-24";
        else if (age <= 29) return "25-29";
        else if (age <= 34) return "30-34";
        else if (age <= 39) return "35-39";
        else if (age <= 49) return "40-49";
        else return "50+";
    }


}
