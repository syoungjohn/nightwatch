package com.jzsec.rtc.bean;

/**
 * Created by caodaoxi on 16-6-30.
 */
public class Epl {
    private int id;
    private String eplName;
    private String eplSql;
    private boolean isAlarm;
    private String groupKeys;
    public Epl() {
    }

    public Epl(int id, String eplName, String eplSql, boolean isAlarm, String groupKeys) {
        this.id = id;
        this.eplName = eplName;
        this.eplSql = eplSql;
        this.isAlarm = isAlarm;
        this.groupKeys = groupKeys;
    }



    public Epl(String eplName, String eplSql) {
        this.eplName = eplName;
        this.eplSql = eplSql;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public boolean isAlarm() {
        return isAlarm;
    }

    public void setAlarm(boolean alarm) {
        isAlarm = alarm;
    }

    public String getGroupKeys() {
        return groupKeys;
    }

    public void setGroupKeys(String groupKeys) {
        this.groupKeys = groupKeys;
    }
}
