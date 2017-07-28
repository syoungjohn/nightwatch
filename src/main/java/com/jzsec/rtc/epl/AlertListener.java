package com.jzsec.rtc.epl;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.jzsec.rtc.bean.Alarm;
import com.jzsec.rtc.util.DBUtils;
import com.jzsec.rtc.util.DateUtils;
//import com.jzsec.rtc.util.SMSClient;
import com.jzsec.rtc.util.TextUtils;

import java.util.Map;

/**
 * Created by caodaoxi on 16-6-30.
 */
public class AlertListener implements UpdateListener {

    private Alarm alarm;
    private int alarmEplId;
    private int eplId;
    private String eplName;
//    private SMSClient smsClient = null;
    public AlertListener(int alarmEplId, int eplId, String eplName, Alarm alarm) {
        this.alarm = alarm;
        this.alarmEplId = alarmEplId;
        this.eplId = eplId;
        this.eplName = eplName;
//        this.smsClient = new SMSClient();
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
            Map<String, Object> statisticEvent = (Map<String, Object>) newEvents[0].getUnderlying();
            String alertTemplate = alarm.getAlertTemplate();
            String message = TextUtils.replaceStrVar(statisticEvent, alertTemplate);
            String startTime = statisticEvent.containsKey("start_time") ? DateUtils.getDateTime(Long.parseLong(statisticEvent.get("start_time").toString())) : DateUtils.getDateTime(System.currentTimeMillis());
            String endTime = statisticEvent.containsKey("end_time") ? DateUtils.getDateTime(Long.parseLong(statisticEvent.get("end_time").toString())) : DateUtils.getDateTime(System.currentTimeMillis());
            System.out.println(message);
            if(alarm.getAlertType() == 1) {
//               this.smsClient.send(alert.getPhone(), message);
                DBUtils.insertAlarmRecord(alarmEplId, eplId, eplName, message, startTime, endTime);
            } else {

            }
        }
    }
}
