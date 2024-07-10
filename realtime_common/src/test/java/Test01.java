import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.bean.TableProcessDim;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class Test01 {
    public static void main(String[] args){
        //1.构建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        //2.添加检查点和状态后端参数
//        // 2.1 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//
//        // 2.2 开启 checkpoint
//        env.enableCheckpointing(5000);
//        // 2.3 设置 checkpoint 模式: 精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 2.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2024/stream/" + "test01");
//        // 2.5 checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 2.6 checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // 2.7 checkpoint  的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 2.8 job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        //3.读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("182.92.236.234:9092")
                        .setTopics("topic_db")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId("test01")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.noWatermarks(),"kafka_source");
        //4.对数据源进行处理
        kafkaSource.print();
        //5.执行环境
        try {
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

