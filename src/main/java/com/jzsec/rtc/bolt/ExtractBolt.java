package com.jzsec.rtc.bolt;


import com.jzsec.rtc.bean.Schema;
import com.jzsec.rtc.epl.EPLManager;
import com.jzsec.rtc.util.TextUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by caodaoxi on 16-4-11.
 */
public class ExtractBolt implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractBolt.class);
    private OutputCollector collector;
    private EPLManager eplManager = null;
    private JSONObject json = null;
    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        eplManager = new EPLManager(stormConf, collector, new CountDownLatch(1));
        this.collector = collector;

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        if(eplManager.isUpdate()) {
            try {
                eplManager.getCountDownLatch().await();
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String record = input.getValue(0).toString();
        json = JSONObject.fromObject(record);
        if (json.containsKey("funcid") && TextUtils.isNumeric(json.getString("funcid").trim())
                && eplManager.getSchema(Integer.parseInt(json.getString("funcid"))) != null
                && eplManager.getSchema(Integer.parseInt(json.getString("funcid"))).size() > 0
                && eplManager.getGroupKeys(Integer.parseInt(json.getString("funcid"))) != null
                && eplManager.getGroupKeys(Integer.parseInt(json.getString("funcid"))).size() > 0) {
            Map<String, Object> schemaMap = null;
            if(json.containsKey("rc_msg_para")) {
                String rcMsgPara = json.getString("rc_msg_para");
                schemaMap = JSONObject.fromObject(rcMsgPara);
                for(Object fieldName : json.keySet()) {
                    if(!schemaMap.containsKey(fieldName) && !"rc_msg_para".equals(fieldName.toString().trim())) {
                        schemaMap.put(fieldName.toString(), json.get(fieldName));
                    }
                }
            } else {
                schemaMap = json;
            }
            int funcid = Integer.parseInt(json.getString("funcid"));
//            if(funcid == 100000 && !schemaMap.containsKey("market")){
//                collector.ack(input);
//                return;
//            }
            //过滤掉非股票交易
            if(schemaMap.containsKey("market") && (schemaMap.containsKey("stkcode") || schemaMap.containsKey("zqdm"))) {
                String market = schemaMap.get("market").toString();
                String stkcode = schemaMap.get("stkcode") != null ? schemaMap.get("stkcode").toString() : schemaMap.get("zqdm").toString();
                if((!"1".equals(market) && !"0".equals(market))
                        ||("1".equals(market) && !stkcode.startsWith("60"))
                        || ("0".equals(market) && !stkcode.startsWith("00") && !stkcode.startsWith("30"))) {
                    collector.ack(input);
                    return;
                }
            }
            if(!schemaMap.containsKey("timestamp")) {
                schemaMap.put("timestamp", System.currentTimeMillis());
            } else {
                schemaMap.put("timestamp", Long.parseLong(schemaMap.get("timestamp").toString()));
            }

            List<Schema> schemas = eplManager.getSchema(funcid);
            StringBuilder keyStr = null;
            for(Schema schema : schemas) {
                String[] keys = schema.getFieldGroupKey().split("\\|");
                keyStr = new StringBuilder();
                for (String key : keys) {
                    keyStr.append(schemaMap.get(key));
                }
                schemaMap.put("tableName", schema.getExtSchemaName());
                System.out.println("tableName : " + schema.getExtSchemaName() + ", key: " + keyStr.toString() + ", Values : " + JSONObject.fromObject(schemaMap).toString());
                collector.emit(new Values(keyStr.toString(), schemaMap));
            }
            keyStr = null;
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashKeys", "schemaMap"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
