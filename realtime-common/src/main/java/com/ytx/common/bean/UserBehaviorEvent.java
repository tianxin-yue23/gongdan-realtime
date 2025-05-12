package com.ytx.common.bean;

public class UserBehaviorEvent {
        private Long userId;       // 用户ID
        private String behaviorType; // 行为类型：view/search/favor/cart/buy
        private Long itemId;       // 商品ID
        private Long categoryId;   // 类目ID
        private Long brandId;      // 品牌ID
        private Double price;      // 商品价格
        private Long timestamp;    // 行为时间戳
        private String deviceType; // 设备类型：iOS/Android/PC
        private String osVersion;  // 操作系统版本
        private String searchKeyword; // 搜索关键词

}
