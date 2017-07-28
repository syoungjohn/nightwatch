package com.jzsec.rtc.bean;

import java.util.Map;

/**
 * Created by caodaoxi on 16-6-30.
 */
public class Schema {
    private int id;
    private String schemaName;
    private String createSchemaSql;
    private String extSchemaName;
    private Map<String, String> fieldMap;
    private String fieldGroupKey;

    public Schema() {
    }

    public Schema(int id, String schemaName, String createSchemaSql) {
        this.id = id;
        this.schemaName = schemaName;
        this.createSchemaSql = createSchemaSql;
    }

    public Schema(int id, String schemaName, String createSchemaSql,String extSchemaName) {
        this.id = id;
        this.schemaName = schemaName;
        this.createSchemaSql = createSchemaSql;
        this.extSchemaName = extSchemaName;
    }

    public Schema(int id, String schemaName, String createSchemaSql, String extSchemaName, Map<String, String> fieldMap, String fieldGroupKey) {
        this.id = id;
        this.schemaName = schemaName;
        this.createSchemaSql = createSchemaSql;
        this.extSchemaName = extSchemaName;
        this.fieldMap = fieldMap;
        this.fieldGroupKey = fieldGroupKey;
    }


    public Schema(String schemaName, String createSchemaSql, int isAlert) {

        this.schemaName = schemaName;
        this.createSchemaSql = createSchemaSql;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getCreateSchemaSql() {
        return createSchemaSql;
    }

    public void setCreateSchemaSql(String createSchemaSql) {
        this.createSchemaSql = createSchemaSql;
    }

    public Map<String, String> getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(Map<String, String> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public String getExtSchemaName() {
        return extSchemaName;
    }

    public void setExtSchemaName(String extSchemaName) {
        this.extSchemaName = extSchemaName;
    }

    public String getFieldGroupKey() {
        return fieldGroupKey;
    }

    public void setFieldGroupKey(String fieldGroupKey) {
        this.fieldGroupKey = fieldGroupKey;
    }
}
