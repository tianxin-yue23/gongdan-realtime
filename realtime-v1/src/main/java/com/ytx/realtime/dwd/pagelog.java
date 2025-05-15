package com.ytx.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ytx.common.constant.Constant;
import com.ytx.common.util.FlinkSinkUtil;
import com.ytx.realtime.util.ConfigUtils;
import com.ytx.realtime.util.DimBaseCategory;
import com.ytx.realtime.util.JdbcUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.sql.Connection;
import java.util.List;

public class pagelog {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD);
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from realtime_v1.base_category3 as b3  \n" +
                    "     join realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("tianxin_yue_dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrds = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> pagelog = kafkaStrds.map(JSON::parseObject);
//        pagelog.print();

//        KafkaSource<String> source3 = KafkaSource.<String>builder()
//                .setBootstrapServers("cdh02:9092")
//                .setTopics("tianxin_yue_base_category_info")
//                .setGroupId("my-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        DataStreamSource<String> kafkaStrus = env.fromSource(source3, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        SingleOutputStreamOperator<JSONObject> basecatrge = kafkaStrus.map(JSON::parseObject);
//        basecatrge.print();


        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = pagelog.map(new MapDeviceInfoAndSearchKetWordMsgFunc());
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());
//        processStagePageLogDs.print();

        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

        // 设备打分模型
        SingleOutputStreamOperator<JSONObject> mapped = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
        mapped.print();
//                mapped.map(js -> js.toJSONString())
//                .sinkTo(
//                        FlinkSinkUtil.getKafkaSink(
//                                Constant.TOPIC_PAGE_INFO)
//                );

        env.execute();

    }
}
