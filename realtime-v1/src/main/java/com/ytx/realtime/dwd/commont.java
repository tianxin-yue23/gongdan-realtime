package com.ytx.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class commont {
    private static final Map<String, Map<String, Double>> PRICE_WEIGHTS = new HashMap<>();
    static {
        // 低价商品权重
        Map<String, Double> lowWeights = new HashMap<>();
        lowWeights.put("18-24", 0.8);
        lowWeights.put("25-29", 0.6);
        lowWeights.put("30-34", 0.4);
        lowWeights.put("35-39", 0.3);
        lowWeights.put("40-49", 0.2);
        lowWeights.put("50+", 0.1);
        PRICE_WEIGHTS.put("low", lowWeights);

        // 中价商品权重
        Map<String, Double> midWeights = new HashMap<>();
        midWeights.put("18-24", 0.2);
        midWeights.put("25-29", 0.4);
        midWeights.put("30-34", 0.6);
        midWeights.put("35-39", 0.7);
        midWeights.put("40-49", 0.8);
        midWeights.put("50+", 0.7);
        PRICE_WEIGHTS.put("mid", midWeights);

        // 高价商品权重
        Map<String, Double> highWeights = new HashMap<>();
        highWeights.put("18-24", 0.1);
        highWeights.put("25-29", 0.2);
        highWeights.put("30-34", 0.3);
        highWeights.put("35-39", 0.4);
        highWeights.put("40-49", 0.5);
        highWeights.put("50+", 0.6);
        PRICE_WEIGHTS.put("high", highWeights);
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取kafa数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("tianxin_yue_user_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        SingleOutputStreamOperator<JSONObject> userInfoStream = kafkaStrDS.map(JSON::parseObject);
//        userInfoStream.print();

        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("realtime-gd-danyu")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
//  订单明细
        DataStreamSource<String> kafkadb = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> orderdetailds = kafkadb.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_detail"));
//        orderdetailds.print();
//    订单表
        SingleOutputStreamOperator<JSONObject> orderinfods = kafkadb.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_info"));
        orderinfods.print();
//userInfoStream关联订单金额，计算年龄段 求出低价商品--低于1000
//中价商品--1001-4000
//高价商品 --＞4000
        SingleOutputStreamOperator<JSONObject> userInfoStream = kafkaStrDS.map(JSON::parseObject)
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject user, ProcessFunction<JSONObject, JSONObject>.Context ctx, org.apache.flink.util.Collector<JSONObject> out) throws Exception {
                        int age = user.getInteger("age");
                        user.put("age_group", calculateAgeGroup(age));
                        out.collect(user);
                    }
                });
//        userInfoStream.print();
//        {"birthday":"1980-11-23","gender":"M","create_time":"1745439758000","age_group":"40-49","zodiac_sign":"射手座",
//        "weight":92,"birth_year":"1980","uid":1052,"login_name":"i6xecv3vqo3","unit_height":"cm","name":"邹致",
//        "user_level":1,"id":1052,"unit_weight":"kg","ts_ms":1747016080663,"age":44,"height":173}

        SingleOutputStreamOperator<JSONObject> orderInfoStream = orderinfods.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject afterobj = jsonObject.getJSONObject("after");
                if (afterobj != null && afterobj.containsKey("total_amount")) {
                    Object amountObj = afterobj.get("total_amount");
                    if (amountObj != null) {
                        double amount = ((Number) amountObj).doubleValue();
                        afterobj.put("price_interval", getPriceInterval(amount));
                    }
                }
                return afterobj;
            }
        });
//       orderInfoStream.print();
//       {"payment_way":"3501","refundable_time":1746055420000,"original_total_amount":8197.0,"order_status":"1002",
//       "consignee_tel":"13535631299","trade_body":"Apple iPhone 12 (A2404) 64GB 黑色 支持移动联通电信5G 双卡双待手机等1件商品",
//       "id":3709,"operate_time":1745450659000,"consignee":"成绍","create_time":1745450620000,"coupon_reduce_amount":0.0,"out_trade_no":"839634498972681",
//       "total_amount":8197.0,"user_id":569,"province_id":16,"price_interval":"high","activity_reduce_amount":0.0}

        DataStream<JSONObject> joinedStream = userInfoStream
                .keyBy(user -> user.getInteger("uid"))
                .intervalJoin(orderInfoStream.keyBy(order -> order.getInteger("user_id")))
                .between(Time.days(-1), Time.minutes(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject user, JSONObject order, Context ctx, Collector<JSONObject> out) {
                        JSONObject result = new JSONObject();
                        result.put("age_group", user.getString("age_group"));
                        result.put("price_interval", order.getString("price_interval"));

                        // 根据年龄组和价格区间获取权重并添加到结果中
                        if (user.containsKey("age_group")) {
                            String ageGroup = user.getString("age_group");
                            String priceInterval = order.getString("price_interval");

                            if (priceInterval != null && ageGroup != null) {
                                Map<String, Double> weightMap = PRICE_WEIGHTS.get(priceInterval);
                                if (weightMap != null) {
                                    Double weight = weightMap.get(ageGroup);
                                    if (weight != null) {
                                        result.put("weight", weight);
                                        out.collect(result);
                                    }
                                }
                            }
                        }

                    }
                });

        joinedStream.print();


        // 打印结果


        env.execute();
    }

    private static String calculateAgeGroup(int age) {
        if (age < 18) return "Under 18";
        else if (age <= 24) return "18-24";
        else if (age <= 29) return "25-29";
        else if (age <= 34) return "30-34";
        else if (age <= 39) return "35-39";
        else if (age <= 49) return "40-49";
        else return "50+";
    }
    // 价格区间权重表

    private static String getPriceInterval(double amount) {
        if (amount <= 1000) return "low";
        else if (amount <= 4000) return "mid";
        else return "high";
    }
}
