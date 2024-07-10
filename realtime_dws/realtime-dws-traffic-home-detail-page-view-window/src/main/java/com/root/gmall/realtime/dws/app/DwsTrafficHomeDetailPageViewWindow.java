package com.root.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.base.BaseApp;
import com.root.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.function.DorisMapFunction;
import com.root.gmall.realtime.common.util.DateFormatUtil;
import com.root.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023,4,"dws_traffic_home_detail_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1.读取dwd层page主题
        //2.清洗过滤
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSONObject.parseObject(value);
                    JSONObject page = jsonObj.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    String mid = jsonObj.getJSONObject("common").getString("mid");
                    if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                        if (mid != null){
                            out.collect(jsonObj);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据：" + value);
                }
            }
        });
        //3.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
        //4.判断独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeLastLoginDesc = new ValueStateDescriptor<>("home_last_login", String.class);
                homeLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                homeLastLoginState = getRuntimeContext().getState(homeLastLoginDesc);

                ValueStateDescriptor<String> detailLastLoginDesc = new ValueStateDescriptor<>("detail_last_login", String.class);
                homeLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                detailLastLoginState = getRuntimeContext().getState(detailLastLoginDesc);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                //判断独立访客 --> 状态存储的日期和当前数据的日期
                String pageId = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                Long homeUvCt = 0L;
                Long goodDetailUvCt = 0L;
                if ("home".equals(pageId)) {
                    String homelastLoginDt = homeLastLoginState.value();
                    if (homelastLoginDt == null || !homelastLoginDt.equals(curDt)) {
                        homeUvCt = 1L;
                        homeLastLoginState.update(curDt);
                    }
                } else {
                    String detailLastLoginDt = detailLastLoginState.value();
                    if (detailLastLoginDt == null || !detailLastLoginDt.equals(curDt)) {
                        goodDetailUvCt = 1L;
                        detailLastLoginState.update(curDt);
                    }
                }
                //如果两个独立访客的度量值都为0 过滤
                if (homeUvCt != 0 || goodDetailUvCt != 0) {
                    out.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvCt(goodDetailUvCt)
                            .ts(ts)
                            .build());
                }
            }
        });
        //5.添加水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWaterMarkStream = processBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficHomeDetailPageViewBean element, long l) {
                return element.getTs();
            }
        }));
        //6.开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        //将度量值合并
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setCurDate(curDt);
                            out.collect(value);
                        }
                    }
                });
        //7.写出到doris
        reduceStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }
}
