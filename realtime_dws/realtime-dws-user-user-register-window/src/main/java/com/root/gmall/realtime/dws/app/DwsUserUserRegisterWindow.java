package com.root.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.base.BaseApp;
import com.root.gmall.realtime.common.bean.UserRegisterBean;
import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.function.DorisMapFunction;
import com.root.gmall.realtime.common.util.DateFormatUtil;
import com.root.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(10025,1,"dws_user_user_register_window", Constant.TOPIC_DWD_USER_REGISTER);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //stream.print();
        //1.读取dwd主题数据
        //2.清洗过滤数据
        //3.转换数据结构
        SingleOutputStreamOperator<UserRegisterBean> beanStream = stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String value, Collector<UserRegisterBean> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String createTime = jsonObject.getString("create_time");
                    String id = jsonObject.getString("id");
                    if (createTime != null && id != null) {
                        out.collect(new UserRegisterBean("", "", "", 1L,createTime));
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据：" + value);
                }
            }
        });
        //4.注册水位线
        SingleOutputStreamOperator<UserRegisterBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long l) {
                return DateFormatUtil.dateTimeToTs(element.getCreateTime());
            }
        }));
        //5.开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> reduceStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> elements, Collector<UserRegisterBean> out) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserRegisterBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            out.collect(element);
                        }
                    }
                });
        reduceStream.print();
        //6.写入到doris
        reduceStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW));
    }
}
