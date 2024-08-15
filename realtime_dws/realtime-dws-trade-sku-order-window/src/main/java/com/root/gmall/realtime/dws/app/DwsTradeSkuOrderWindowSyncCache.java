package com.root.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.base.BaseApp;
import com.root.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.function.DorisMapFunction;
import com.root.gmall.realtime.common.util.DateFormatUtil;
import com.root.gmall.realtime.common.util.FlinkSinkUtil;
import com.root.gmall.realtime.common.util.HBaseUtil;
import com.root.gmall.realtime.common.util.RedisUtil;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;

public class DwsTradeSkuOrderWindowSyncCache extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(10029,4,"dws_trade_sku_order_window_sync_cache", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
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
        //reduceBeanStream.print();
        //6.关联维度信息
        //6.1关联sku_info 补充维度信息
        reduceBeanStream.map(new RichMapFunction<TradeSkuOrderBean,TradeSkuOrderBean>() {
            Connection connection;
            Jedis jedis;
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getConnection();
                jedis = RedisUtil.getJedis();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
                RedisUtil.closeJedis(jedis);
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
                //1.拼接对应的redisKey
                String redisKey = RedisUtil.getRedisKey("dim_sku_info", bean.getSkuId());
                //2.读取redis缓存数据
                String dim = jedis.get(redisKey);
                JSONObject dimSkuInfo;
                //3.判断redis读取到的数据是否为空
                if (dim == null || dim.length() == 0){
                    //redis中没有缓存  需要读取HBase
                    System.out.println("没有缓存，读取HBase"+redisKey);
                    dimSkuInfo = HBaseUtil.getCells(connection,Constant.HBASE_NAMESPACE,"dim_sku_info", bean.getSkuId());
                    //存储到redis
                    if (dimSkuInfo.size()!=0){
                        jedis.setex(redisKey,24*60*60,dimSkuInfo.toJSONString());
                    }
                }else {
                    System.out.println("有缓存，直接返回redis数据"+redisKey);
                    dimSkuInfo = JSONObject.parseObject(dim);
                }
                if (dimSkuInfo.size()!=0){
                    bean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                    bean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                    bean.setSpuId(dimSkuInfo.getString("spu_id"));
                    bean.setSkuName(dimSkuInfo.getString("sku_name"));
                }else {
                    System.out.println("没有对应的维度信息"+bean);
                }
                return bean;
            }
        });
//        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimBeanStream = reduceBeanStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
//            Connection connection;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                connection = HBaseUtil.getConnection();
//            }
//
//            @Override
//            public void close() throws Exception {
//                HBaseUtil.closeConnection(connection);
//            }
//
//            @Override
//            public TradeSkuOrderBean map(TradeSkuOrderBean bean) throws Exception {
//                //(1)使用hbase的api读取表格的数据
//                JSONObject dimSkuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", bean.getSkuId());
//                //(2)使用读取到的字段补全信息
//                bean.setCategory3Id(dimSkuInfo.getString("category3_id"));
//                bean.setTrademarkId(dimSkuInfo.getString("tm_id"));
//                bean.setSpuId(dimSkuInfo.getString("spu_id"));
//                bean.setSkuName(dimSkuInfo.getString("sku_name"));
//                //关联spu表格
//                JSONObject dimSpuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_spu_info", bean.getSpuId());
//                bean.setSpuName(dimSpuInfo.getString("spu_name"));
//                //关联C3表格
//                JSONObject dimC3 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category3", bean.getCategory3Id());
//                bean.setCategory3Name(dimC3.getString("name"));
//                bean.setCategory2Id(dimC3.getString("category2_id"));
//                //关联C2表格
//                JSONObject dimC2 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category2", bean.getCategory2Id());
//                bean.setCategory2Name(dimC2.getString("name"));
//                bean.setCategory1Id(dimC2.getString("category1_id"));
//                //关联C1表格
//                JSONObject dimC1 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category1", bean.getCategory1Id());
//                bean.setCategory1Name(dimC1.getString("name"));
//                //关联品牌表
//                //关联C3表格
//                JSONObject dimTm = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_trademark", bean.getTrademarkId());
//                bean.setTrademarkName(dimTm.getString("tm_name"));
//                return bean;
//            }
//        });//.print
//        //7.写出到doris
//        fullDimBeanStream.map(new DorisMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));
    }
}
