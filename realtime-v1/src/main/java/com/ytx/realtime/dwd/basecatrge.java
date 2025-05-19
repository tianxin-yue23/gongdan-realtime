package com.ytx.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class basecatrge {
    // 维度权重
    private static final double CATEGORY_PREFERENCE_WEIGHT = 0.3;
    private static final double BRAND_PREFERENCE_WEIGHT = 0.2;
    private static final double PRICE_SENSITIVITY_WEIGHT = 0.15;
    private static final double TIME_BEHAVIOR_WEIGHT = 0.1;


    // 类目偏好权重映射
    private static final Map<String, Map<String, Double>> CATEGORY_PREFERENCE_WEIGHTS = new HashMap<>();
    // 品牌偏好权重映射
    private static final Map<String, Map<String, Double>> BRAND_PREFERENCE_WEIGHTS = new HashMap<>();
    // 价格敏感度权重映射
    private static final Map<String, Map<String, Double>> PRICE_SENSITIVITY_WEIGHTS = new HashMap<>();
    // 时间行为权重映射
    private static final Map<String, Map<String, Double>> TIME_PERIOD_WEIGHTS = new HashMap<>();
//     时间格式化器
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        // 低价商品权重
        Map<String, Double> lowWeights = new HashMap<>();
        lowWeights.put("18-24", 0.8);
        lowWeights.put("25-29", 0.6);
        lowWeights.put("30-34", 0.4);
        lowWeights.put("35-39", 0.3);
        lowWeights.put("40-49", 0.2);
        lowWeights.put("50+", 0.1);
        PRICE_SENSITIVITY_WEIGHTS.put("low", lowWeights);

        // 中价商品权重
        Map<String, Double> midWeights = new HashMap<>();
        midWeights.put("18-24", 0.2);
        midWeights.put("25-29", 0.4);
        midWeights.put("30-34", 0.6);
        midWeights.put("35-39", 0.7);
        midWeights.put("40-49", 0.8);
        midWeights.put("50+", 0.7);
        PRICE_SENSITIVITY_WEIGHTS.put("mid", midWeights);

        // 高价商品权重
        Map<String, Double> highWeights = new HashMap<>();
        highWeights.put("18-24", 0.1);
        highWeights.put("25-29", 0.2);
        highWeights.put("30-34", 0.3);
        highWeights.put("35-39", 0.4);
        highWeights.put("40-49", 0.5);
        highWeights.put("50+", 0.6);
        PRICE_SENSITIVITY_WEIGHTS.put("high", highWeights);
    }
    private static void initializeCategoryPreferenceWeights() {
        // 潮流服饰类目权重
        Map<String, Double> fashionWeights = new HashMap<>();
        fashionWeights.put("18-24", 0.9);
        fashionWeights.put("25-29", 0.8);
        fashionWeights.put("30-34", 0.6);
        fashionWeights.put("35-39", 0.4);
        fashionWeights.put("40-49", 0.2);
        fashionWeights.put("50+", 0.1);
        CATEGORY_PREFERENCE_WEIGHTS.put("电脑办公", fashionWeights);

        Map<String, Double> dianqiWeights = new HashMap<>();
        fashionWeights.put("18-24", 0.9);
        fashionWeights.put("25-29", 0.8);
        fashionWeights.put("30-34", 0.6);
        fashionWeights.put("35-39", 0.4);
        fashionWeights.put("40-49", 0.2);
        fashionWeights.put("50+", 0.1);
        CATEGORY_PREFERENCE_WEIGHTS.put("家用电器", dianqiWeights);

        Map<String, Double> homeWeights = new HashMap<>();
        homeWeights.put("18-24", 0.2);
        homeWeights.put("25-29", 0.4);
        homeWeights.put("30-34", 0.6);
        homeWeights.put("35-39", 0.8);
        homeWeights.put("40-49", 0.9);
        homeWeights.put("50+", 0.7);
        CATEGORY_PREFERENCE_WEIGHTS.put("个护化妆", homeWeights);

        // 健康食品类目权重
        Map<String, Double> healthFoodWeights = new HashMap<>();
        healthFoodWeights.put("18-24", 0.1);
        healthFoodWeights.put("25-29", 0.2);
        healthFoodWeights.put("30-34", 0.4);
        healthFoodWeights.put("35-39", 0.6);
        healthFoodWeights.put("40-49", 0.8);
        healthFoodWeights.put("50+", 0.9);
        CATEGORY_PREFERENCE_WEIGHTS.put("手机", healthFoodWeights);
    }

    private static void initializeBrandPreferenceWeights() {

        Map<String, Double> zaraWeights = new HashMap<>();
        zaraWeights.put("18-24", 0.9);
        zaraWeights.put("25-29", 0.7);
        zaraWeights.put("30-34", 0.5);
        zaraWeights.put("35-39", 0.3);
        zaraWeights.put("40-49", 0.2);
        zaraWeights.put("50+", 0.1);
        BRAND_PREFERENCE_WEIGHTS.put("香奈儿", zaraWeights);


        Map<String, Double> hailanWeights = new HashMap<>();
        hailanWeights.put("18-24", 0.1);
        hailanWeights.put("25-29", 0.3);
        hailanWeights.put("30-34", 0.5);
        hailanWeights.put("35-39", 0.7);
        hailanWeights.put("40-49", 0.8);
        hailanWeights.put("50+", 0.9);
        BRAND_PREFERENCE_WEIGHTS.put("小米", hailanWeights);

        Map<String, Double> lianxiangWeights = new HashMap<>();
        hailanWeights.put("18-24", 0.1);
        hailanWeights.put("25-29", 0.3);
        hailanWeights.put("30-34", 0.5);
        hailanWeights.put("35-39", 0.7);
        hailanWeights.put("40-49", 0.8);
        hailanWeights.put("50+", 0.9);
        BRAND_PREFERENCE_WEIGHTS.put("联想", lianxiangWeights);

        Map<String, Double> xiaomiWeights = new HashMap<>();
        hailanWeights.put("18-24", 0.1);
        hailanWeights.put("25-29", 0.3);
        hailanWeights.put("30-34", 0.5);
        hailanWeights.put("35-39", 0.7);
        hailanWeights.put("40-49", 0.8);
        hailanWeights.put("50+", 0.9);
        BRAND_PREFERENCE_WEIGHTS.put("Redmi", xiaomiWeights);
    }
    private static void initializeTimePeriodWeights() {
        // 凌晨权重
        Map<String, Double> earlyMorningWeights = new HashMap<>();
        earlyMorningWeights.put("18-24", 0.2);
        earlyMorningWeights.put("25-29", 0.1);
        earlyMorningWeights.put("30-34", 0.1);
        earlyMorningWeights.put("35-39", 0.1);
        earlyMorningWeights.put("40-49", 0.1);
        earlyMorningWeights.put("50+", 0.1);
        TIME_PERIOD_WEIGHTS.put("凌晨", earlyMorningWeights);

        // 早晨权重
        Map<String, Double> morningWeights = new HashMap<>();
        morningWeights.put("18-24", 0.1);
        morningWeights.put("25-29", 0.1);
        morningWeights.put("30-34", 0.1);
        morningWeights.put("35-39", 0.1);
        morningWeights.put("40-49", 0.2);
        morningWeights.put("50+", 0.3);
        TIME_PERIOD_WEIGHTS.put("早晨", morningWeights);

        // 上午权重
        Map<String, Double> forenoonWeights = new HashMap<>();
        forenoonWeights.put("18-24", 0.2);
        forenoonWeights.put("25-29", 0.2);
        forenoonWeights.put("30-34", 0.2);
        forenoonWeights.put("35-39", 0.2);
        forenoonWeights.put("40-49", 0.3);
        forenoonWeights.put("50+", 0.4);
        TIME_PERIOD_WEIGHTS.put("上午", forenoonWeights);

        // 中午权重
        Map<String, Double> noonWeights = new HashMap<>();
        noonWeights.put("18-24", 0.4);
        noonWeights.put("25-29", 0.4);
        noonWeights.put("30-34", 0.4);
        noonWeights.put("35-39", 0.4);
        noonWeights.put("40-49", 0.4);
        noonWeights.put("50+", 0.3);
        TIME_PERIOD_WEIGHTS.put("中午", noonWeights);

        // 下午权重
        Map<String, Double> afternoonWeights = new HashMap<>();
        afternoonWeights.put("18-24", 0.4);
        afternoonWeights.put("25-29", 0.5);
        afternoonWeights.put("30-34", 0.5);
        afternoonWeights.put("35-39", 0.5);
        afternoonWeights.put("40-49", 0.5);
        afternoonWeights.put("50+", 0.4);
        TIME_PERIOD_WEIGHTS.put("下午", afternoonWeights);

        // 晚上权重
        Map<String, Double> eveningWeights = new HashMap<>();
        eveningWeights.put("18-24", 0.8);
        eveningWeights.put("25-29", 0.7);
        eveningWeights.put("30-34", 0.6);
        eveningWeights.put("35-39", 0.5);
        eveningWeights.put("40-49", 0.4);
        eveningWeights.put("50+", 0.3);
        TIME_PERIOD_WEIGHTS.put("晚上", eveningWeights);

        // 夜间权重
        Map<String, Double> nightWeights = new HashMap<>();
        nightWeights.put("18-24", 0.9);
        nightWeights.put("25-29", 0.7);
        nightWeights.put("30-34", 0.5);
        nightWeights.put("35-39", 0.3);
        nightWeights.put("40-49", 0.2);
        nightWeights.put("50+", 0.1);
        TIME_PERIOD_WEIGHTS.put("夜间", nightWeights);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source3 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("tianxin_yue_base_category_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrus = env.fromSource(source3, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> basecatrge = kafkaStrus.map(JSON::parseObject);
//        basecatrge.print();

        SingleOutputStreamOperator<JSONObject> resultStream = basecatrge.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    // 提取年龄组
                    String ageGroup = jsonObject.getString("age_group");
                    // 提取类目名称
                    String categoryName = jsonObject.getString("category1_name");
                    // 提取品牌名称
                    String brandName = jsonObject.getString("tm_name");
                    // 提取价格区间
                    String priceInterval = jsonObject.getString("price_interval");
                    // 提取时间段（假设数据中有相关字段）
                    String timePeriod = jsonObject.getString("create_time");
                    // 确保数据包含所需字段
                    if (ageGroup != null && categoryName != null && brandName != null && priceInterval != null && timePeriod != null) {
                        // 解析时间并分类时段
                        LocalTime time = parseTime(timePeriod);
                        String timePeriods = classifyTimePeriod(time);                        // 计算类目偏好得分
                        double categoryPreferenceScore = getScore(CATEGORY_PREFERENCE_WEIGHTS, categoryName, ageGroup);
                        // 计算品牌偏好得分
                        double brandPreferenceScore = getScore(BRAND_PREFERENCE_WEIGHTS, brandName, ageGroup);
                        // 计算价格敏感度得分
                        double priceSensitivityScore = getScore(PRICE_SENSITIVITY_WEIGHTS, priceInterval, ageGroup);
                        // 计算时间行为得分
                        double timeBehaviorScore = getScore(TIME_PERIOD_WEIGHTS, timePeriods, ageGroup);


                        double combinedScore =categoryPreferenceScore * CATEGORY_PREFERENCE_WEIGHT;
                        double pinpai =brandPreferenceScore * BRAND_PREFERENCE_WEIGHT;
                        double jiage =priceSensitivityScore * PRICE_SENSITIVITY_WEIGHT;
                        double shijian =timeBehaviorScore * TIME_BEHAVIOR_WEIGHT;

                        // 添加综合得分到JSON对象
                        jsonObject.put("combined_score", combinedScore);
                        jsonObject.put("pinpai", pinpai);
                        jsonObject.put("jiage", jiage);
                        jsonObject.put("shijian", shijian);
                        jsonObject.put("time_period", timePeriods);
                        jsonObject.put("parsed_time", time.format(DateTimeFormatter.ISO_LOCAL_TIME)); // 添加解析后的时间
                        // 输出处理后的对象
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing JSON: " + jsonObject + ", error: " + e.getMessage());
                }
            }

            private double getScore(Map<String, Map<String, Double>> weightsMap, String key, String ageGroup) {
                Map<String, Double> weights = weightsMap.get(key);
                if (weights != null) {
                    Double weight = weights.get(ageGroup);
                    if (weight != null) {
                        return weight;
                    }
                }
                return 0.5; // 默认得分
            }
        });

        // 输出结果
//        resultStream.print();
        KafkaSource<String> source4 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("tianxin_yue_page_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrus1 = env.fromSource(source4, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> pageinfods = kafkaStrus1.map(JSON::parseObject);
//        pageinfods.print();

        DataStream<JSONObject> join = resultStream.join(pageinfods)
                .where(data -> data.getString("uid"))
                .equalTo(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply((first, second) -> {
                    JSONObject result = new JSONObject();
                    result.put("result",first);
                    result.put("Log",second);
                    return result;
                });
//        join.print();


//        根据各个年龄段对类目品牌搜索词，时间，价格区间，设备的权重做总和
        SingleOutputStreamOperator<JSONObject> tmp = join.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = jsonObject.getJSONObject("result");
                JSONObject log = jsonObject.getJSONObject("Log");
                String[] fields1 = {"shijian", "jiage", "pinpai", "combined_score"};
                String[] fields2 = {"device", "search"};
                BigDecimal sum = BigDecimal.ZERO;

                String ageGroup = result.getString("age_group");
                for (String s : fields1) {
                    BigDecimal bigDecimal = result.getBigDecimal(s);
                    sum = sum.add(bigDecimal != null ? bigDecimal : BigDecimal.ZERO);
                }

                for (String s : fields2) {
                    BigDecimal bigDecimal = log.getBigDecimal(s + "_" + ageGroup);
                    sum = sum.add(bigDecimal != null ? bigDecimal : BigDecimal.ZERO);
                }

                result.put("sum", sum);
                sum = sum.setScale(3, RoundingMode.HALF_UP);
                double aDouble = sum.doubleValue();
//                System.out.println(aDouble);
                if (aDouble >= 0.75) {
                    result.put("new_ageGroup", "18-24");
                } else if (aDouble >= 0.69) {
                    result.put("new_ageGroup", "25-29");
                } else if (aDouble >= 0.585) {
                    result.put("new_ageGroup", "30-34");
                } else if (aDouble >= 0.47) {
                    result.put("new_ageGroup", "35-39");
                } else if (aDouble >= 0.365) {
                    result.put("new_ageGroup", "40-49");
                } else if (aDouble >= 0.26) {
                    result.put("new_ageGroup", "50+");
                }else {
                    result.put("new_ageGroup", "0-17");
                }

                return jsonObject;
            }
        });
        tmp.print();
//        tmp.writeAsText( "C:\\Users/86131/Desktop/output.csv").setParallelism(1);
//        "C:\\Users/86131/Desktop/output.csv"

        env.execute();
    }
    private static LocalTime parseTime(String timeStr) {
        try {
            // 先尝试解析为时间戳（长整型）
            long timestamp = Long.parseLong(timeStr);
            // 假设是毫秒级时间戳
            return Instant.ofEpochMilli(timestamp)
                    .atZone(ZoneId.systemDefault())
                    .toLocalTime();
        } catch (NumberFormatException e) {
            try {
                // 尝试解析为日期时间字符串
                return LocalTime.parse(timeStr, DATETIME_FORMATTER);
            } catch (Exception e1) {
                try {
                    // 尝试解析为时间字符串
                    return LocalTime.parse(timeStr, TIME_FORMATTER);
                } catch (Exception e2) {
                    // 默认返回当前时间
                    return LocalTime.now();
                }
            }
        }
    }
    private static String classifyTimePeriod(LocalTime time) {
        int hour = time.getHour();
        if (hour >= 0 && hour < 6) return "凌晨";
        else if (hour < 9) return "早晨";
        else if (hour < 12) return "上午";
        else if (hour < 14) return "中午";
        else if (hour < 18) return "下午";
        else if (hour < 22) return "晚上";
        else return "夜间";
    }

}
