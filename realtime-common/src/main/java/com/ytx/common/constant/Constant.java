package com.ytx.common.constant;

public class Constant {
    public static final String KAFKA_BROKERS = "cdh02:9092,cdh01:9092,cdh03:9092";

    public static final String TOPIC_DB = "realtime-gd-danyu";
//            "realtime-gd-danyu";

//    realtime-gd-danyu
    public static final String TOPICGL = "realtime_v2_db";
    public static final String TOPICOMMONT = "kafka_comment_tianxinyue";
    public static final String TOPIC_LOG = "tianxin_yue_log";
    public static final String TOPIC_USER = "tianxin_yue_user_info";
    public static final String TOPIC_CATAGE_INFO = "tianxin_yue_base_category_info";

    public static final String TOPIC_PAGE_INFO = "tianxin_yue_page_info";
    public static final String TOPIC_CATAGE = "tianxin_yue_base_category";
    public static final String MYSQL_HOST = "10.160.60.17";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "ns_tianxin_yue";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "tianxin_yue_dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "tianxin_yue_dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "tianxin_yue_dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "tianxin_yue_dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "tianxin_yue_dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "tianxin_yue_dwd_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "tianxin_yue_DWD_TRADE_CART_ADD";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "tianxin_yuedwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "tianxin_yue_dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "tianxin_yue_dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "tianxin_yue_dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "tianxin_yue_dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES = "10.160.60.14:8030";

    public static final String DORIS_DATABASE = "realtime_v2_tianxin_yue";

}
