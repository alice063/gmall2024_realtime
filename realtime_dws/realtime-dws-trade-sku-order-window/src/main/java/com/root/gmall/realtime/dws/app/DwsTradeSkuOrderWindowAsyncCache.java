package com.root.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.base.BaseApp;
import com.root.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.function.DimAsyncFunction;
import com.root.gmall.realtime.common.function.DorisMapFunction;
import com.root.gmall.realtime.common.util.DateFormatUtil;
import com.root.gmall.realtime.common.util.FlinkSinkUtil;
import com.root.gmall.realtime.common.util.HBaseUtil;
import com.root.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.eclipse.jetty.util.Callback;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class DwsTradeSkuOrderWindowAsyncCache extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowAsyncCache().start(10029,4,"dws_trade_sku_order_window_async_cache", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务逻辑
        //1.读取DWD层下单主题数据
        //stream.print();
        //2.过滤清洗
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    if (value != null) {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        Long ts = jsonObject.getLong("ts");
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        if (ts != null && id != null && skuId != null) {
                            jsonObject.put("ts",ts*1000);
                            out.collect(jsonObject);
                        }
                    }
                }catch (Exception e){
                    System.out.println("过滤掉脏数据"+value);
                }
            }
        });
        //jsonObjectStream.print();
        //3.添加水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = jsonObjectStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        //4.修正度量值 转换数据结构
        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });
        SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountDesc = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmountDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                //调取状态中的度量值
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");

//                if (orderAmount!=null){
//                    System.out.println("相同的id"+jsonObject);
//                }

                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;
                BigDecimal curOriginalAmount = jsonObject.getBigDecimal("order_price").multiply(jsonObject.getBigDecimal("sku_num"));
                TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(jsonObject.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();
                //存储当前的度量值
                lastAmountState.put("originalAmount",curOriginalAmount);
                lastAmountState.put("activityReduceAmount",jsonObject.getBigDecimal("split_activity_amount"));
                lastAmountState.put("couponReduceAmount",jsonObject.getBigDecimal("split_coupon_amount"));
                lastAmountState.put("orderAmount",jsonObject.getBigDecimal("split_total_amount"));
                collector.collect(bean);
            }
        });
        //processBeanStream.print();
        //5.分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = processBeanStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
                    @Override
                    public String getKey(TradeSkuOrderBean value) throws Exception {
                        return value.getSkuId();
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeSkuOrderBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            out.collect(element);
                        }
                    }
                });
        //6.关联维度信息
        //6.1异步IO关联sku_info 补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(reduceBeanStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getSkuId();
            }

            @Override
            public String getTableName() {
                return "dim_sku_info";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject dim) {
                input.setCategory3Id(dim.getString("category3_id"));
                input.setTrademarkId(dim.getString("tm_id"));
                input.setSpuId(dim.getString("spu_id"));
                input.setSkuName(dim.getString("sku_name"));
            }
        }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeSkuOrderBean> spuInfoStream = AsyncDataStream.unorderedWait(skuInfoStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getSpuId();
            }
            @Override
            public String getTableName() {
                return "dim_spu_info";
            }
            @Override
            public void join(TradeSkuOrderBean input, JSONObject dim) {
                input.setSpuName(dim.getString("spu_name"));
            }
        }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = AsyncDataStream.unorderedWait(spuInfoStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            public String getTableName() {
                return "dim_base_trademark";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject dim) {
                input.setTrademarkName(dim.getString("tm_name"));
            }
        }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(tmStream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject dim) {
                input.setCategory3Name(dim.getString("name"));
                input.setCategory2Id(dim.getString("category2_id"));
            }
        }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(c3Stream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject dim) {
                input.setCategory2Name(dim.getString("name"));
                input.setCategory1Id(dim.getString("category1_id"));
            }
        }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimStream = AsyncDataStream.unorderedWait(c2Stream, new DimAsyncFunction<TradeSkuOrderBean>() {
            @Override
            public String getId(TradeSkuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject dim) {
                input.setCategory1Name(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);
        fullDimStream.print();
        //7.写出到doris
        fullDimStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));
    }
}
