package com.root.gmall.realtime.dwd.db.app;

import com.root.gmall.realtime.common.base.BaseSQLApp;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014,4,"dwd_trade_order_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        //添加状态的存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(5L));
        //1.读取topic_db数据
        createTopicDb(ckAndGroupId,tableEnv);
        //2.筛选订单详情表数据
        Table odTable = tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['sku_name'] sku_name," +
                "data['order_price'] order_price," +
                "data['sku_num'] sku_num," +
                "data['create_time'] create_time," +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                "data['split_total_amount'] split_total_amount," +  // 分摊总金额
                "data['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                "data['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                "ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail' " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail",odTable);
        //3.筛选订单信息表数据
        Table oiTable = tableEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_info",oiTable);
        //4.筛选订单详情活动关联表
        Table odaTable = tableEnv.sqlQuery("select " +
                "data['order_detail_id'] id," +
                "data['activity_id'] activity_id," +
                "data['activity_rule_id'] activity_rule_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_activity' " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_activity",odaTable);
        //5.筛选订单详情优惠券关联表
        Table odcTable = tableEnv.sqlQuery("select " +
                "data['order_detail_id'] id," +
                "data['coupon_id'] coupon_id " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_coupon",odcTable);
        //6.合并表格
        Table joinTable = tableEnv.sqlQuery("select " +
                "od.id," +
                "order_id," +
                "sku_id," +
                "user_id," +
                "province_id," +
                "activity_id," +
                "activity_rule_id," +
                "coupon_id," +
                "sku_name," +
                "order_price," +
                "sku_num," +
                "create_time," +
                "split_total_amount," +
                "split_activity_amount," +
                "split_coupon_amount," +
                "ts " +
                "from order_detail od " +
                "join order_info oi on od.order_id=oi.id " +
                "left join order_detail_activity oda " +
                "on od.id=oda.id " +
                "left join order_detail_coupon odc " +
                "on od.id=odc.id ");
        //7.写出到kafka对应主题中
        //使用left join会产生撤回流 将数据写入kafka必须使用upsert kafka
        tableEnv.executeSql("create table dwd_trade_order_detail(" +
                "id string," +
                "order_id string," +
                "sku_id string," +
                "user_id string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "sku_name string," +
                "order_price string," +
                "sku_num string," +
                "create_time string," +
                "split_total_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "ts bigint," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ")"+ SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }
}
