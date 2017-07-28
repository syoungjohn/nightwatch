package com.jzsec.rtc.epl;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;
import com.jzsec.rtc.bean.Epl;
import com.jzsec.rtc.util.DBUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by caodaoxi on 16-7-26.
 */
public class StatisticListener implements UpdateListener {

    private Epl epl;
    private OutputCollector collector;
    public StatisticListener(Epl epl, OutputCollector collector) {
        this.epl = epl;
        this.collector = collector;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
            EventType eventType = newEvents[0].getEventType();
            String outputStreamName = eventType.getName();
            Map<String, Object> statisticEvent = (Map<String, Object>) newEvents[0].getUnderlying();
            if(epl.isAlarm()) collector.emit(new Values(outputStreamName, statisticEvent));
            DBUtils.insertTriggerRecord(this.epl.getId(), this.epl.getEplName(), statisticEvent);
        }
    }
}
