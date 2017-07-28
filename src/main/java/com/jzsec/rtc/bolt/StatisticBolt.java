package com.jzsec.rtc.bolt;



import com.espertech.esper.client.EPException;
import com.jzsec.rtc.bean.Schema;
import com.jzsec.rtc.epl.EPLManager;
import net.sf.json.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by caodaoxi on 16-4-11.
 */
public class StatisticBolt implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(StatisticBolt.class);
    private OutputCollector collector;
    private EPLManager eplManager = null;
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
        Map<String, Object> record = (Map<String, Object>) input.getValue(1);


        if (record.containsKey("funcid") && eplManager.getSchema(Integer.parseInt(record.get("funcid").toString())) != null) {
            try {
                eplManager.getEPRuntime().sendEvent(record, record.get("tableName").toString());
            } catch (EPException e) {
                System.out.println(JSONObject.fromObject(record).toString());
                e.printStackTrace();
            }
        }


        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("EventName","StatisticEvent"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
