package com.root.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topicName){
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("root-" + topicName + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }
    public static KafkaSink<JSONObject> getKafkaSinkWithTopicName(){
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaSinkContext context, Long timestamp) {
                        String topicName = element.getString("sink_table");
                        element.remove("sink_table");
                        return new ProducerRecord<>(topicName,Bytes.toBytes(element.toJSONString()));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("root-"+"base_db"+System.currentTimeMillis())
                .setProperty("transaction.timeout.ms",15*60*1000+"")
                .build();
    }
    public static DorisSink<String> getDorisSink(String tableName){
        Properties properties = new Properties();
        properties.setProperty("format","json");
        properties.setProperty("read_json_by_line","true");
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris"+System.currentTimeMillis())
                        .setDeletable(false)
                        .setStreamLoadProp(properties).build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.FENODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE+"."+tableName)
                        .setUsername(Constant.DORIS_USERNAME)
                        .setPassword(Constant.DORIS_PASSWORD)
                        .build())
                .build();
    }
}
