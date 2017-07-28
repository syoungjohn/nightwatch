package com.jzsec.rtc.bean;

import java.util.Map;

/**
 * Created by caodaoxi on 16-4-12.
 */
public class Parameter {
    private String groupKey;
    private String msgId;
    private long timestamp;
    private String businessScope;
    private Map<String, Double> statistic;

    public Parameter(String groupKey, String msgId, long timestamp, String businessScope, Map<String, Double> statistic) {
        this.groupKey = groupKey;
        this.msgId = msgId;
        this.timestamp = timestamp;
        this.businessScope = businessScope;
        this.statistic = statistic;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getBusinessScope() {
        return businessScope;
    }

    public void setBusinessScope(String businessScope) {
        this.businessScope = businessScope;
    }

    public Map<String, Double> getStatistic() {
        return statistic;
    }

    public void setStatistic(Map<String, Double> statistic) {
        this.statistic = statistic;
    }
}
