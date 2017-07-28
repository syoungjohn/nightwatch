package com.jzsec.rtc.bean;

/**
 * Created by caodaoxi on 16-7-21.
 */
public class AlarmEpl {
    private int alarmEplId;
    private int eplId;
    private String eplName;
    private String eplSql;
    private Alarm alarm;

    public AlarmEpl(int alarmEplId, int eplId, String eplName, String eplSql, Alarm alarm) {
        this.alarmEplId = alarmEplId;
        this.eplId = eplId;
        this.eplName = eplName;
        this.eplSql = eplSql;
        this.alarm = alarm;
    }

    public int getAlarmEplId() {
        return alarmEplId;
    }

    public void setAlarmEplId(int alarmEplId) {
        this.alarmEplId = alarmEplId;
    }

    public int getEplId() {
        return eplId;
    }

    public void setEplId(int eplId) {
        this.eplId = eplId;
    }

    public String getEplName() {
        return eplName;
    }

    public void setEplName(String eplName) {
        this.eplName = eplName;
    }

    public String getEplSql() {
        return eplSql;
    }

    public void setEplSql(String eplSql) {
        this.eplSql = eplSql;
    }

    public Alarm getAlarm() {
        return alarm;
    }

    public void setAlarm(Alarm alarm) {
        this.alarm = alarm;
    }
}
