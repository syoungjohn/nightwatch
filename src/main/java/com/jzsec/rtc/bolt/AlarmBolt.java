package com.jzsec.rtc.bolt;


import com.jzsec.rtc.epl.AlarmEPLManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by caodaoxi on 16-4-11.
 */
public class AlarmBolt implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AlarmBolt.class);
    private OutputCollector collector;
    private AlarmEPLManager alarmEPLManager = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.alarmEPLManager = new AlarmEPLManager(stormConf, new CountDownLatch(1));
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        if(alarmEPLManager.isUpdate()) {
            try {
                alarmEPLManager.getCountDownLatch().await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String streamName = input.getValue(0).toString();
        Map<String, Object> record = (Map<String, Object>) input.getValue(1);
        alarmEPLManager.getEPRuntime().sendEvent(record, streamName);
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
