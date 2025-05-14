package com.ytx.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytx.common.constant.Constant;


import com.ytx.common.util.FlinkSinkUtil;
import com.ytx.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.Period;
import java.time.format.DateTimeFormatter;


public class Userlog  {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取kafa数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("realtime-gd-danyu")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //读取kafa数据
        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("tianxin_yue_user_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrds = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> userInfo = kafkaStrds.map(JSON::parseObject);
//        userInfoStream.print();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> filtered = kafkaStrDS.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));
//        filtered.print();
//  订单明细
        DataStreamSource<String> kafkadb = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> orderdetailds = kafkadb.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_detail"));
//        orderdetailds.print();
//    订单表
        SingleOutputStreamOperator<JSONObject> orderinfods = kafkadb.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_info"));
//        orderinfods.print();

        SingleOutputStreamOperator<JSONObject> userweight = kafkaStrDS.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
//            userweight.print();

        SingleOutputStreamOperator<JSONObject> userInfoStream = filtered.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject afterobj = jsonObject.getJSONObject("after");
                if (afterobj != null && afterobj.containsKey("birthday")) {
                    Object birthdayValue = afterobj.get("birthday");
                    if (birthdayValue != null) {
                        try {
                            long days = Long.parseLong(birthdayValue.toString());
                            LocalDate baseDate = LocalDate.of(1970, 1, 1);
                            LocalDate birthDate = baseDate.plusDays(days);

                            // 格式化日期
                            String formatDate = birthDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
                            afterobj.put("birthday", formatDate);

                            // 计算年龄
                            int age = Period.between(birthDate, LocalDate.now()).getYears();
                            afterobj.put("age", age);

                            // 添加星座和出生年份
                            afterobj.put("zodiac_sign", getZodiacSign(birthDate));
                            afterobj.put("birth_year", birthDate.getYear());

                        } catch (NumberFormatException e) {
                            afterobj.put("birthday", "invalid_date");
                            afterobj.put("age", -1);
                            afterobj.put("zodiac_sign", "unknown");
                            afterobj.put("birth_year", -1);
                        }
                    }
                }
                return jsonObject;
            }
        });
//        userInfoStream.print();
        SingleOutputStreamOperator<JSONObject> result2 = userInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");

                Integer age = after.getInteger("age");
                Integer user_level = after.getInteger("user_level");
                Long ts_ms = jsonObject.getLong("ts_ms");
                String birthday = after.getString("birthday");
                String gender = after.getString("gender");
                String name = after.getString("name");
                String zodiacSign = after.getString("zodiac_sign");
                String createTime = after.getString("create_time");
                Integer id = after.getInteger("id");
                String birthDecade = after.getString("birth_year");
                String login_name = after.getString("login_name");
                object.put("birthday", birthday);
                object.put("birth_year", birthDecade);
                object.put("name", name);
                object.put("ts_ms", ts_ms);
                object.put("zodiac_sign", zodiacSign);
                object.put("id", id);
                object.put("user_level", user_level);
                object.put("gender", gender);
                object.put("age", age);
                object.put("create_time", createTime);
                object.put("login_name", login_name);
                return object;
            }
        });

//       result2.print();
        SingleOutputStreamOperator<JSONObject> userInfoDs = userweight.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = jsonObject.getJSONObject("after");

                Integer height = after.getInteger("height");
                String unit_height = after.getString("unit_height");
                Integer weight = after.getInteger("weight");
                String unit_weight = after.getString("unit_weight");
                Integer uid = after.getInteger("uid");
                Long ts = jsonObject.getLong("ts_ms");
                object.put("height", height);
                object.put("unit_height", unit_height);
                object.put("weight", weight);
                object.put("unit_weight", unit_weight);
                object.put("uid", uid);
                object.put("ts_ms", ts);
                return object;
            }
        });
//        userInfoDs.print();

// 按uid进行keyBy
        SingleOutputStreamOperator<JSONObject> operator = result2.keyBy(json -> json.getInteger("id")).intervalJoin(userInfoDs.keyBy(json -> json.getInteger("uid")))
                .between(Time.days(-1), Time.days(1)).process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        collector.collect(jsonObject);
                    }
                });
//        operator.print();
//        写进kafka
//        operator.map(js -> js.toJSONString())
//                .sinkTo(
//                        FlinkSinkUtil.getKafkaSink(
//                                Constant.TOPIC_USER)
//                );

        SingleOutputStreamOperator<JSONObject> result3 = orderdetailds.keyBy(data -> data.getJSONObject("after").getInteger("order_id")).intervalJoin(orderinfods.keyBy(data -> data.getJSONObject("after").getInteger("id")))
                .between(Time.days(-1), Time.days(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject detail, JSONObject info, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        result.put("id", detail.getJSONObject("after").getInteger("id"));
                        result.put("order_id", detail.getJSONObject("after").getInteger("order_id"));
                        result.put("sku_id", detail.getJSONObject("after").getInteger("sku_id"));
                        result.put("sku_name", detail.getJSONObject("after").getString("sku_name"));
                        result.put("sku_num", detail.getJSONObject("after").getInteger("sku_num"));
                        result.put("create_time", info.getJSONObject("after").getString("create_time"));
                        result.put("user_id", info.getJSONObject("after").getInteger("user_id"));
                        result.put("total_amount", info.getJSONObject("after").getBigDecimal("total_amount"));
                        result.put("original_total_amount", info.getJSONObject("after").getBigDecimal("original_total_amount"));
                        result.put("ts_ms", info.getLong("ts_ms"));
                        collector.collect(result);
                    }
                });
//        result3.print();
//      关联sku信息
        SingleOutputStreamOperator<JSONObject> sku = result3.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("sku_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("sku_name", skuInfoJsonObj.getString("sku_name"));
                        object.put("tm_id", skuInfoJsonObj.getString("tm_id"));
                        object.put("category3_id", skuInfoJsonObj.getString("category3_id"));
                        return object;
                    }

                }
        );
//        sku.print();
       SingleOutputStreamOperator<JSONObject> tm = sku.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("tm_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_trademark", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("tm_name", skuInfoJsonObj.getString("tm_name"));
//                        object.put("category3_id", skuInfoJsonObj.getString("category3_id"));
                        return object;
                    }
                }
        );
//        tm.print();
//        关联三级品类
         SingleOutputStreamOperator<JSONObject> category3 = tm.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;
                    private Connection hbaseConn2;
                    private Connection hbaseConn3;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category3_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_category3", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("category3_name", skuInfoJsonObj.getString("name"));
                        object.put("category2_id", skuInfoJsonObj.getString("category2_id"));
                        return object;
                    }
                }
        );
//        category3.print();

//         关联2级品类
        SingleOutputStreamOperator<JSONObject> category2 = category3.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                      HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category2_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_category2", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("category2_name", skuInfoJsonObj.getString("name"));
                        object.put("category1_id", skuInfoJsonObj.getString("category1_id"));
                        return object;
                    }

                }
        );
//        category2.print();
//        1级
        SingleOutputStreamOperator<JSONObject> category1 = category2.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category1_id");
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_category1", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("category1_name", skuInfoJsonObj.getString("name"));
                        return object;
                    }
                }
        );
//        category1.print();

        //        category1.map(js -> js.toJSONString())
//                .sinkTo(
//                        FlinkSinkUtil.getKafkaSink(
//                                Constant.TOPIC_CATAGE)
//                );
        SingleOutputStreamOperator<JSONObject> mapped = operator.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

//                计算年龄区间
                int age = jsonObject.getInteger("age");
                jsonObject.put("age_group", calculateAgeGroup(age));
                return jsonObject;
            }
        });
//        mapped.print();
        SingleOutputStreamOperator<JSONObject> orderInfoStream = category1.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                BigDecimal totalAmount = jsonObject.getBigDecimal("total_amount");
                jsonObject.put("price_interval", getPriceInterval(totalAmount.doubleValue()));

                return jsonObject;
            }
        });
//       orderInfoStream.print();
        DataStream<JSONObject> joinedStream = orderInfoStream
                .keyBy(user -> user.getInteger("user_id"))
                .intervalJoin(mapped.keyBy(order -> order.getInteger("uid")))
                .between(Time.days(-1), Time.days(1))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject user, JSONObject order, Context ctx, Collector<JSONObject> out) {
                        JSONObject result = new JSONObject();
                        result.putAll(order);
                        result.putAll(user);
                        out.collect(result);
                    }
                });

        joinedStream.print();

        env.execute("Userlog");
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

    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        MonthDay md = MonthDay.of(month, day);

        if (md.isAfter(MonthDay.of(12, 21))) return "摩羯座";
        else if (md.isAfter(MonthDay.of(11, 22))) return "射手座";
        else if (md.isAfter(MonthDay.of(10, 23))) return "天蝎座";
        else if (md.isAfter(MonthDay.of(9, 22))) return "天秤座";
        else if (md.isAfter(MonthDay.of(8, 22))) return "处女座";
        else if (md.isAfter(MonthDay.of(7, 22))) return "狮子座";
        else if (md.isAfter(MonthDay.of(6, 21))) return "巨蟹座";
        else if (md.isAfter(MonthDay.of(5, 20))) return "双子座";
        else if (md.isAfter(MonthDay.of(4, 19))) return "金牛座";
        else if (md.isAfter(MonthDay.of(3, 20))) return "白羊座";
        else if (md.isAfter(MonthDay.of(2, 18))) return "双鱼座";
        else if (md.isAfter(MonthDay.of(1, 19))) return "水瓶座";
        else return "摩羯座";
    }
    private static String getPriceInterval(double amount) {
        if (amount <= 1000) return "low";
        else if (amount <= 4000) return "mid";
        else return "high";
    }

}