package com.jzsec.rtc.bean;

/**
 * Created by caodaoxi on 16-6-30.
 */
public class Alarm {
    private int alarmEplId;
    private int eplId;
    private int alertType;
    private String phone;
    private String email;
    private String alertTemplate;

    public Alarm(int alarmEplId, int eplId, int alertType, String phone, String email, String alertTemplate) {
        this.alarmEplId = alarmEplId;
        this.eplId = eplId;
        this.alertType = alertType;
        this.phone = phone;
        this.email = email;
        this.alertTemplate = alertTemplate;
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

    public int getAlertType() {
        return alertType;
    }

    public void setAlertType(int alertType) {
        this.alertType = alertType;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAlertTemplate() {
        return alertTemplate;
    }

    public void setAlertTemplate(String alertTemplate) {
        this.alertTemplate = alertTemplate;
    }
}
