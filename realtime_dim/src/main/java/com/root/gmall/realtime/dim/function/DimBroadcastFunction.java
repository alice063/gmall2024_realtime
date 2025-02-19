package com.root.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.root.gmall.realtime.common.bean.TableProcessDim;
import com.root.gmall.realtime.common.util.JDBCUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    public HashMap<String,TableProcessDim> hashMap;
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //预加载初始的维度表信息
        java.sql.Connection myConnection = JDBCUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JDBCUtil.queryList(myConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for(TableProcessDim tableProcessDim : tableProcessDims){
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JDBCUtil.closeConnection(myConnection);
    }

    @Override
    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //读取广播状态
        BroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        //将配置表信息作为一个维度表的标记，写到广播状态
        String op = value.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(value.getSourceTable());
            //同步删除hashMap中初始化加载的配置表信息
            hashMap.remove(value.getSourceTable());
        }else {
            tableProcessState.put(value.getSourceTable(), value);
        }
    }
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        //查询广播状态，判断当前的数据对应的表格是否存在于状态里面
        String tableName = value.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);
        //如果是主流数据先到达造成数据为空
        if(tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
        }
        if (tableProcessDim != null) {
            //状态不为空 说明当前一行数据是维度表数据
            out.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}
