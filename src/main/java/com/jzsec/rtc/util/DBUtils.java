package com.jzsec.rtc.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.jzsec.rtc.bean.*;
import com.jzsec.rtc.config.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.json.simple.JSONObject;

import java.sql.*;
import java.util.*;

/**
 * Created by caodaoxi on 16-4-15.
 */
public class DBUtils {

    private static DruidDataSource dataSource = null;
    static {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(Configuration.getConfig().getString("rtc.drivename"));
        dataSource.setUsername(Configuration.getConfig().getString("rtc.username"));
        dataSource.setPassword(Configuration.getConfig().getString("rtc.password"));
        dataSource.setUrl(Configuration.getConfig().getString("rtc.url"));
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(10);
        dataSource.setValidationQuery("select 1 from dual");
    }

    public static DruidPooledConnection getConnection() {
        DruidPooledConnection con = null;
        try {
            con = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }


    public static Map<Integer, List<Schema>> qetAllSchema(Integer schemaId) {
        String where = schemaId == null ? " " : " where d.schema_id = " + schemaId + "\n";
        String sql = "select d.schema_id as id, e.schema_name, e.ext_schema_name, concat('create schema ', e.ext_schema_name, '(' , d.name_types, ')') as schema_sql, e.field_group_keys, d.field_keys\n" +
                "from\n" +
                "(\n" +
                "select s.schema_id, group_concat(distinct concat(trim(field_name),' ',trim(field_type))) as name_types, group_concat(distinct concat(trim(field_name),':',trim(field_type))) as field_keys\n" +
                "from rtc_xp_schema s \n" +
                "join\n" +
                "rtc_schema_field f \n" +
                "on s.schema_id=f.schema_id\n" +
                "group by s.schema_id,schema_name\n" +
                ") d\n" +
                "join \n" +
                "(\n" +
                "select s.schema_id, schema_name, concat(schema_name, '_', t.group_keys) ext_schema_name, t.group_keys, t.field_group_keys\n" +
                "from rtc_xp_schema s \n" +
                "join\n" +
                "(\n" +
                "select distinct schema_id,group_concat(schema_field_name ORDER BY schema_field_name DESC separator  '_') as group_keys, group_concat(schema_field_name ORDER BY schema_field_name DESC separator  '|') as field_group_keys\n" +
                "from rtc_epl_group_key\n" +
                "join rtc_epl\n" +
                "on rtc_epl_group_key.epl_id=rtc_epl.epl_id\n" +
                "where status=1\n" +
                "group by rtc_epl_group_key.epl_id,schema_id\n" +
                ") t\n" +
                "on s.schema_id=t.schema_id\n" +
                ") e\n" +
                "on d.schema_id=e.schema_id " + where;
        Connection con = getConnection();
        Map<Integer, List<Schema>> schemeMap = new HashMap<Integer, List<Schema>>();
        if (con == null) return schemeMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            st.execute("SET @@session.group_concat_max_len = 10000;");
            rs = st.executeQuery(sql);
            Schema scheme = null;
            while (rs.next()) {
                int id = rs.getInt("id");
                String schemaName = rs.getString("schema_name");
                String extSchemaName = rs.getString("ext_schema_name");
                String schemaSql = rs.getString("schema_sql");
                Map<String, String> fieldMap = new HashMap<String, String>();
                String[] fieldKeys = rs.getString("field_keys").split(",");
                for(String fieldKey : fieldKeys) {
                    String[] nameType = fieldKey.split(":");
                    if(nameType.length != 2) {
                        System.out.println(fieldKey);
                    }
                    fieldMap.put(nameType[0].trim(), nameType[1].trim());
                }
                String fieldGroupKeys = rs.getString("field_group_keys");
                scheme = new Schema(id, schemaName, schemaSql, extSchemaName, fieldMap, fieldGroupKeys);
                if(schemeMap.containsKey(id)) {
                    schemeMap.get(id).add(scheme);
                } else {
                    List<Schema> schemas = new ArrayList<Schema>();
                    schemas.add(scheme);
                    schemeMap.put(id, schemas);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return schemeMap;
    }

    public static Map<Integer, List<Schema>> qetAllEplSchema(Integer eplId) {
        String where = eplId == null ? " " : " where d.epl_id = " + eplId + "\n";
        String sql = "select epl_id, s.schema_id, schema_name, concat(schema_name, '_', t.group_keys) ext_schema_name\n" +
                "from rtc_xp_schema s \n" +
                "join\n" +
                "(\n" +
                "select distinct schema_id,rtc_epl_group_key.epl_id as epl_id, group_concat(schema_field_name ORDER BY schema_field_name DESC separator  '_') as group_keys, group_concat(schema_field_name ORDER BY schema_field_name DESC separator  '|') as field_group_keys\n" +
                "from rtc_epl_group_key\n" +
                "join rtc_epl\n" +
                "on rtc_epl_group_key.epl_id=rtc_epl.epl_id\n" +
                "where status=1\n" +
                "group by rtc_epl_group_key.epl_id,schema_id\n" +
                ") t\n" +
                "on s.schema_id=t.schema_id " + where;
        Connection con = getConnection();
        Map<Integer, List<Schema>> schemeMap = new HashMap<Integer, List<Schema>>();
        if (con == null) return schemeMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            Schema scheme = null;
            while (rs.next()) {
                int id = rs.getInt("epl_id");
                String schemaName = rs.getString("schema_name");
                String extSchemaName = rs.getString("ext_schema_name");
                String schemaSql = rs.getString("schema_sql");
                Map<String, String> fieldMap = new HashMap<String, String>();
                String[] fieldKeys = rs.getString("field_keys").split(",");
                for(String fieldKey : fieldKeys) {
                    String[] nameType = fieldKey.split(":");
                    if(nameType.length != 2) {
                        System.out.println(fieldKey);
                    }
                    fieldMap.put(nameType[0].trim(), nameType[1].trim());
                }
                String fieldGroupKeys = rs.getString("field_group_keys");
                scheme = new Schema(id, schemaName, schemaSql, extSchemaName, fieldMap, fieldGroupKeys);
                if(schemeMap.containsKey(id)) {
                    schemeMap.get(id).add(scheme);
                } else {
                    List<Schema> schemas = new ArrayList<Schema>();
                    schemas.add(scheme);
                    schemeMap.put(id, schemas);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return schemeMap;
    }

    public static Map<Integer, Schema> qetAllAlarmSchema() {
        String sql = "select s.schema_id id,schema_name,concat('create schema ', schema_name, '(',group_concat(concat(field_name,' ',field_type)),')') as schema_sql\n" +
                    "from rtc_alarm_schema s\n" +
                    "left join rtc_alarm_schema_field_relation r\n" +
                    "on(s.schema_id=r.alarm_schema_id)\n" +
                    "left join rtc_alarm_schema_field f\n" +
                    "on r.alarm_schema_field_id=f.id\n" +
                    "group by s.schema_id,schema_name";
        Connection con = getConnection();
        Map<Integer, Schema> alarmSchemeMap = new HashMap<Integer, Schema>();
        if (con == null) return alarmSchemeMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            Schema alarmScheme = null;
            while (rs.next()) {
                int id = rs.getInt("id");
                String schemaName = rs.getString("schema_name");
                String schemaSql = rs.getString("schema_sql");
                alarmScheme = new Schema(id, schemaName, schemaSql);
                alarmSchemeMap.put(id, alarmScheme);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return alarmSchemeMap;
    }


    public static Map<Integer, Epl> qetAllEpls(Integer eplId) {
        String where = eplId == null ? "where status=1\n" : " where e.epl_id = " + eplId + "\n";
        String sql = "select e.epl_id id,e.epl_name,e.epl,e.is_alarm, concat(group_concat(distinct concat(threshold_name,'=',threshold_value))) thresholds, group_concat(distinct concat(schema_name,'|',g.ext_schema)) as all_schema\n" +
                "from \n" +
                "rtc_epl e\n" +
                "left join \n" +
                "rtc_threshold t\n" +
                "on e.epl_id=t.epl_id\n" +
                "join \n" +
                "(\n" +
                "select s.schema_id id, epl_id, schema_name, concat(schema_name, '_', t.group_keys) ext_schema, t.group_keys\n" +
                "from rtc_xp_schema s \n" +
                "join\n" +
                "(\n" +
                "select distinct epl_id,schema_id,group_concat(schema_field_name separator  '_') as group_keys\n" +
                "from rtc_epl_group_key\n" +
                "group by epl_id,schema_id\n" +
                ") t\n" +
                "on s.schema_id=t.schema_id\n" +
                "group by s.schema_id,schema_name,epl_id\n" +
                ") g\n" +
                "on g.epl_id=e.epl_id\n" + where +
                "group by e.epl_id,e.epl_name,e.epl";
        Connection con = getConnection();
        Map<Integer, Epl> eplMap = new HashMap<Integer, Epl>();
        if (con == null) return eplMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            Epl epl = null;
            while (rs.next()) {
                int id = rs.getInt("id");
                String eplName = rs.getString("epl_name");
                String eplSql = rs.getString("epl");
                String thresholds = rs.getString("thresholds");
                boolean isAlarm = rs.getInt("is_alarm") == 1 ? true : false;
                if(thresholds != null && !StringUtils.isEmpty(thresholds)) {
                    String[] dts = thresholds.split(",");
                    Map<String, Object> paramMap = new HashMap<String, Object>();
                    for(String threshold : dts) {
                        String[] fields = threshold.split("=");
                        paramMap.put(fields[0], fields[1]);
                    }
                    eplSql = TextUtils.replaceStrVar(paramMap, eplSql);
                }
                String[] schemas = rs.getString("all_schema").split(",");
                for(String schemaStr : schemas) {
                    String[] schemaStrArr = schemaStr.split("\\|");
                    eplSql = eplSql.replaceAll(schemaStrArr[0].trim(), schemaStrArr[1].trim());
                }
                epl = new Epl(id, eplName, eplSql, isAlarm, null);
                eplMap.put(id, epl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return eplMap;
    }


    public static Map<Integer, AlarmEpl> qetAllAlarmEpls() {
        String sql = "select m.alarm_epl_id,m.epl_id,m.epl_name,m.epl,m.alarm_type,m.alarm_group_id,m.alarm_template,m.phone,m.email,concat(group_concat(concat(threshold_name,'=',threshold_value)))  thresholds\n" +
                    "from\n" +
                    "(\n" +
                    "\tselect e.epl_id alarm_epl_id,re.epl_id epl_id,e.epl_name,e.epl,e.alarm_type,e.alarm_group_id,e.alarm_template,g.phone,g.email\n" +
                    "\tfrom \n" +
                    "\trtc_alarm_epl e\n" +
                    "\tleft join rtc_alarm_epl_relation r\n" +
                    "\ton (e.epl_id=r.alarm_epl_id)\n" +
                    "\tleft join rtc_epl re\n" +
                    "\ton (r.epl_id=re.epl_id)\n" +
                    "\tleft join\n" +
                    "\t(\n" +
                    "\tselect g.group_id id, GROUP_CONCAT(phone) phone,GROUP_CONCAT(email) email\n" +
                    "\tfrom rtc_alarm_group g\n" +
                    "\tleft outer join rtc_alarm_group_user gu\n" +
                    "\ton(g.group_id=gu.group_id)\n" +
                    "\tleft outer join rtc_alarm_user u\n" +
                    "\ton(gu.user_id=u.user_id)\n" +
                    "\tgroup by g.group_id\n"+
                    "\t) g\n" +
                    "\ton(e.alarm_group_id=g.id)\n" +
                    ") m\n" +
                    "left join\n" +
                    "rtc_alarm_threshold t\n" +
                    "on m.alarm_epl_id=t.epl_id\n" +
                    "group by m.alarm_epl_id,m.epl_id,m.epl_name,m.epl,m.alarm_type,m.alarm_group_id,m.alarm_template,m.phone,m.email";
        Connection con = getConnection();
        Map<Integer, AlarmEpl> alarmEplMap = new HashMap<Integer, AlarmEpl>();
        if (con == null) return alarmEplMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            AlarmEpl alarmEpl = null;
            while (rs.next()) {
                int alarmEplId = rs.getInt("alarm_epl_id");
                int eplId = rs.getInt("epl_id");
                String alarmEplName = rs.getString("epl_name");
                String alarmEplSql = rs.getString("epl");

                String thresholds = rs.getString("thresholds");
                if(thresholds != null && !StringUtils.isEmpty(thresholds)) {
                    String[] dts = thresholds.split(",");
                    Map<String, Object> paramMap = new HashMap<String, Object>();
                    for(String threshold : dts) {

                        String[] fields = threshold.split("=");
                        paramMap.put(fields[0], fields[1]);
                    }
                    alarmEplSql = TextUtils.replaceStrVar(paramMap, alarmEplSql);
                }
                Alarm alarm = new Alarm(alarmEplId, eplId, rs.getInt("alarm_type"), rs.getString("phone"), rs.getString("email"), rs.getString("alarm_template"));
                alarmEpl = new AlarmEpl(alarmEplId, eplId, alarmEplName, alarmEplSql, alarm);
                alarmEplMap.put(alarmEplId, alarmEpl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return alarmEplMap;
    }


    public static Map<String, Method> qetAllMethods() {
        String sql = "select id,method_name,cache_reference_type,method_describe,max_age_seconds,purge_interval_seconds from rtc_method";
        Connection con = getConnection();
        Map<String, Method> methodMap = new HashMap<String, Method>();
        if (con == null) return methodMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            Method method = null;
            while (rs.next()) {
                int id = rs.getInt("id");
                String methodName = rs.getString("method_name");
                String cacheReferenceType = rs.getString("cache_reference_type");
                String methodDescribe = rs.getString("method_describe");
                int maxAgeSeconds = rs.getInt("max_age_seconds");
                int purgeIntervalSeconds = rs.getInt("purge_interval_seconds");
                method = new Method(id, methodName, cacheReferenceType, methodDescribe, maxAgeSeconds, purgeIntervalSeconds);
                methodMap.put(methodName, method);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return methodMap;
    }

    public static Map<String, DBInfo> qetAllDBInfos() {
        String sql = "select id,db_props props,db_name,connection_lifecycle,cache_reference_type,db_describe,source_factory,max_age_seconds,purge_interval_seconds from rtc_db";
        Connection con = getConnection();
        Map<String, DBInfo> dbMap = new HashMap<String, DBInfo>();
        if (con == null) return dbMap;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            DBInfo dbInfo = null;
            while (rs.next()) {
                int id = rs.getInt("id");
                String dbName = rs.getString("db_name");
                String[] props = rs.getString("props").split(",");
                String connectionLifecycle = rs.getString("connection_lifecycle");
                String cacheReferenceType = rs.getString("cache_reference_type");
                String dbDescribe = rs.getString("db_describe");
                int sourceFactoryType = rs.getInt("source_factory");
                String sourceFactory = "org.apache.commons.dbcp.BasicDataSourceFactory";
                int maxAgeSeconds = rs.getInt("max_age_seconds");
                int purgeIntervalSeconds = rs.getInt("purge_interval_seconds");
                dbInfo = new DBInfo(id, props, dbName, connectionLifecycle, cacheReferenceType, dbDescribe, sourceFactory, maxAgeSeconds, purgeIntervalSeconds);
                dbMap.put(dbName, dbInfo);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, st, rs);
        }
        return dbMap;
    }

    private static void close(Connection con, Statement st, ResultSet rs) {
        try {
            if(rs != null) rs.close();
            if(st != null) st.close();
            if(con != null) con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void close(Connection con, Statement st) {
        try {
            if(st != null) st.close();
            if(con != null) con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void insertTriggerRecord1(Map<String, Object> record, String tableName) {
        if(record == null) return;
        Connection con = getConnection();
        try {
            Statement st = con.createStatement();
            String headSql = "insert into " + tableName + "(";
            Set<String> keys = record.keySet();
            List<String> ks = new ArrayList<String>();
            for(String k : keys) {
                ks.add(k);
                headSql = headSql + k + ",";
            }

            if(headSql.endsWith(",")) {
                headSql = headSql.substring(0, headSql.length() - 1);
            }

            headSql = headSql + ") values";
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            for (String k : ks) {
                if("String".equals(record.get(k).getClass().getSimpleName())) {
                    builder.append("'").append(record.get(k)).append("',");
                } else {
                    builder.append(record.get(k)).append(",");
                }

            }
            String vSql = builder.toString();
            if(vSql.endsWith(",")) {
                vSql = vSql.substring(0, vSql.length() - 1);
            }
            vSql = vSql + ")";
            headSql = headSql + vSql;
            st.addBatch(headSql);
            st.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void insertTriggerRecord(int eplId, String eplName, Map<String, Object> record) {
        if(record == null) return;
        Connection con = getConnection();
        PreparedStatement ps = null;
        String sql = "insert into rtc_rule_trigger_record(epl_id,ymt_code,epl_name,trigger_state,trigger_time,start_time,end_time,level) values(?,?,?,?,?,?,?,?)";
        try {
            ps = con.prepareStatement(sql);
            ps.setInt(1, eplId);
            if(record.containsKey("ymt_code") && !StringUtils.isEmpty(record.get("ymt_code").toString())) {
                ps.setLong(2, Long.parseLong(record.get("ymt_code").toString()));
            } else {
                ps.setLong(2, 0);
            }
            ps.setString(3, eplName);
            ps.setString(4, JSONObject.toJSONString(record));
            ps.setString(5, DateUtils.getCurrentTime());
            if(record.containsKey("start_time")) {
                ps.setString(6, DateUtils.getDateTime(Long.parseLong(record.get("start_time").toString())));
//                record.remove("start_time");
            } else {
                ps.setString(6, DateUtils.getDateTime(System.currentTimeMillis()));
            }
            if(record.containsKey("end_time")) {
                ps.setString(7, DateUtils.getDateTime(Long.parseLong(record.get("end_time").toString())));
//                record.remove("end_time");
            } else {
                ps.setString(7, DateUtils.getDateTime(System.currentTimeMillis()));
            }
            if(record.containsKey("level") && record.get("level") != null) {
                ps.setString(8, record.get("level").toString());
            } else {
                ps.setString(8, "1");
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, ps);
        }
    }


    public static void insertAlarmRecord(int alarmEplId, int eplId, String eplName, String message, String startTime, String endTime) {
        Connection con = getConnection();
        PreparedStatement ps = null;
        String sql = "insert into rtc_alarm_record(alarm_epl_id, epl_id,epl_name,message,alarm_time,start_time,end_time) values(?,?,?,?,?,?,?)";
        try {
            ps = con.prepareStatement(sql);
            ps.setInt(1, alarmEplId);
            ps.setInt(2, eplId);
            ps.setString(3, eplName);
            ps.setString(4, message);
            ps.setString(5, DateUtils.getCurrentTime());
            ps.setString(6, startTime);
            ps.setString(7, endTime);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(con, ps);
        }
    }
}
