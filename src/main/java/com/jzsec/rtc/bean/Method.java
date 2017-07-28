package com.jzsec.rtc.bean;

/**
 * Created by caodaoxi on 16-7-11.
 */
public class Method {
    private int id;
    private String methodName;
    private String cacheReferenceType;
    private String dbDescribe;
    private int maxAgeSeconds;
    private int purgeIntervalSeconds;

    public Method(int id, String methodName, String cacheReferenceType, String dbDescribe, int maxAgeSeconds, int purgeIntervalSeconds) {
        this.id = id;
        this.methodName = methodName;
        this.cacheReferenceType = cacheReferenceType;
        this.dbDescribe = dbDescribe;
        this.maxAgeSeconds = maxAgeSeconds;
        this.purgeIntervalSeconds = purgeIntervalSeconds;
    }

    public Method(String methodName, String cacheReferenceType, String dbDescribe, int maxAgeSeconds, int purgeIntervalSeconds) {
        this.methodName = methodName;
        this.cacheReferenceType = cacheReferenceType;
        this.dbDescribe = dbDescribe;
        this.maxAgeSeconds = maxAgeSeconds;
        this.purgeIntervalSeconds = purgeIntervalSeconds;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getCacheReferenceType() {
        return cacheReferenceType;
    }

    public void setCacheReferenceType(String cacheReferenceType) {
        this.cacheReferenceType = cacheReferenceType;
    }

    public String getDbDescribe() {
        return dbDescribe;
    }

    public void setDbDescribe(String dbDescribe) {
        this.dbDescribe = dbDescribe;
    }

    public int getMaxAgeSeconds() {
        return maxAgeSeconds;
    }

    public void setMaxAgeSeconds(int maxAgeSeconds) {
        this.maxAgeSeconds = maxAgeSeconds;
    }

    public int getPurgeIntervalSeconds() {
        return purgeIntervalSeconds;
    }

    public void setPurgeIntervalSeconds(int purgeIntervalSeconds) {
        this.purgeIntervalSeconds = purgeIntervalSeconds;
    }
}
