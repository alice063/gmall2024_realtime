package com.root.gmall.realtime.common.base;

import com.root.gmall.realtime.common.constant.Constant;
import com.root.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSQLApp {
    public void start(int port,int parallelism,String ckAndGroupId){
        //获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        System.setProperty("HADOOP_USER_NAME","root");
        //1.构建Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        //2.添加检查点和状态后端参数
//        // 2.1 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//
//        // 2.2 开启 checkpoint
//        env.enableCheckpointing(5000);
//        // 2.3 设置 checkpoint 模式: 精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 2.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("file:///d/temp/checkpoint" + ckAndGroupId);//hdfs://182.92.236.234:8020/gmall2024/stream/
//        // 2.5 checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 2.6 checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // 2.7 checkpoint  的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 2.8 job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        //创建tableenv环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.对数据源进行处理
        //kafkaSource.print();
        handle(tableEnv,env,ckAndGroupId);

    }
    public void createTopicDb(String ckAndGroupId, StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(ckAndGroupId));
    }

    public void createBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "   rowkey STRING,\n" +
                "   info ROW<dic_name String>,\n" +
                "   PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector'='hbase-2.2',\n" +
                "   'table-name'='gmall:dim_base_dic',\n" +
                "   'zookeeper.quorum'='"+ Constant.HBASE_ZOOKEEPER_QUORUM +"'\n" +
                ")");
    }
    public abstract void handle(StreamTableEnvironment tableEnv,StreamExecutionEnvironment env,String ckAndGroupId);
}
