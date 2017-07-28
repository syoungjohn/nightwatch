package com.jzsec.rtc.bean;

/**
 * Created by caodaoxi on 16-7-11.
 */
public class DBInfo {
    private int id;
    private String[] props;
    private String dbName;
    private String connectionLifecycle;
    private String cacheReferenceType;
    private String dbDescribe;
    private String sourceFactory;
    private int maxAgeSeconds;
    private int purgeIntervalSeconds;

    public DBInfo(int id, String[] props, String dbName, String connectionLifecycle, String cacheReferenceType, String dbDescribe, String sourceFactory, int maxAgeSeconds, int purgeIntervalSeconds) {
        this.id = id;
        this.props = props;
        this.dbName = dbName;
        this.connectionLifecycle = connectionLifecycle;
        this.cacheReferenceType = cacheReferenceType;
        this.dbDescribe = dbDescribe;
        this.sourceFactory = sourceFactory;
        this.maxAgeSeconds = maxAgeSeconds;
        this.purgeIntervalSeconds = purgeIntervalSeconds;
    }

    public DBInfo(String[] props, String dbName, String connectionLifecycle, String cacheReferenceType, String dbDescribe, String sourceFactory, int maxAgeSeconds, int purgeIntervalSeconds) {
        this.props = props;
        this.dbName = dbName;
        this.connectionLifecycle = connectionLifecycle;
        this.cacheReferenceType = cacheReferenceType;
        this.dbDescribe = dbDescribe;
        this.sourceFactory = sourceFactory;
        this.maxAgeSeconds = maxAgeSeconds;
        this.purgeIntervalSeconds = purgeIntervalSeconds;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String[] getProps() {
        return props;
    }

    public void setProps(String[] props) {
        this.props = props;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getConnectionLifecycle() {
        return connectionLifecycle;
    }

    public void setConnectionLifecycle(String connectionLifecycle) {
        this.connectionLifecycle = connectionLifecycle;
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

    public String getSourceFactory() {
        return sourceFactory;
    }

    public void setSourceFactory(String sourceFactory) {
        this.sourceFactory = sourceFactory;
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
