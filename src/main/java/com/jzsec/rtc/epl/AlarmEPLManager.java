package com.jzsec.rtc.epl;

import com.espertech.esper.client.*;
import com.jzsec.rtc.bean.*;
import com.jzsec.rtc.util.DBUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.storm.Config;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by caodaoxi on 16-6-30.
 */
public class AlarmEPLManager implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(AlarmEPLManager.class);
    private EPAdministrator admin = null;
    private Map<Integer, Schema>  allSchema = null;
    private Map<Integer, AlarmEpl> allEpls = null;
    private Map<String, Method> allMethods = null;
    private Map<String, DBInfo> allDbs = null;
    private EPRuntime runtime = null;
    private EPServiceProvider epService = null;
    private Map<Integer, EPStatement> schemaStateMap = new HashMap<Integer, EPStatement>();
    private Map<Integer, EPStatement> eplStateMap = new HashMap<Integer, EPStatement>();
    private CuratorFramework curator;
    private Map conf = null;
    private Configuration configuration = null;
    private CountDownLatch latch;
    private boolean isUpdate = false;
    public AlarmEPLManager(Map conf, CountDownLatch latch) {

        this.conf = conf;
        this.latch = latch;
        this.configuration = new Configuration();
        this.addMethods(configuration);
        this.addDbs();


        this.epService = EPServiceProviderManager.getDefaultProvider(configuration);
        this.admin = epService.getEPAdministrator();

        this.addSchemas();
        this.addEpls();
        this.start();
        curator = CuratorFrameworkFactory
                .newClient(com.jzsec.rtc.config.Configuration.getConfig().getString("rtc.zk.hosts"),
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                        Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
                        new RetryNTimes(
                                Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                                Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))
                        ));
        curator.start();
        createPath();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(curator, "/alarm", true);
        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                public void childEvent(CuratorFramework framework,
                                       PathChildrenCacheEvent event) throws Exception {
                    List<ChildData> childDataList = pathChildrenCache.getCurrentData();
                    if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED || event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                        if (childDataList != null && childDataList.size() > 0) {
                            // update and reload single rule  by businessScope in future
                            List<Map<Object, Object>> zkDataList = new ArrayList<Map<Object, Object>>();
                            for (ChildData childData : childDataList) {
                                LOG.info("==" + childData.getPath() + " changed," + new String(childData.getData(), "UTF-8"));
                                String data = new String(childData.getData(), "UTF-8");
                                if(!StringUtils.isEmpty(data)) {
                                    System.out.println("==" + childData.getPath() + " changed," + new String(childData.getData(), "UTF-8"));
                                    Map<Object, Object> zkData = (Map<Object, Object>) JSONValue.parse(data);
                                    zkDataList.add(zkData);
                                }

                            }
                            if(zkDataList.size() > 0) refresh(zkDataList);
                        }
                    }
                }
            }, pool);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public AlarmEPLManager addSchemas() {
        this.allSchema = DBUtils.qetAllAlarmSchema();
        Collection<Schema> schemas = allSchema.values();
        for(Schema schema : schemas) {
            this.admin.createEPL(schema.getCreateSchemaSql());
        }
        return this;
    }

    public AlarmEPLManager addDbs() {
        this.allDbs = DBUtils.qetAllDBInfos();
        Collection<DBInfo> dbInfos = this.allDbs.values();
        for (DBInfo dbInfo : dbInfos) {
            Properties pps = new Properties();
            String[] props = dbInfo.getProps();
            ConfigurationDBRef configDB = new ConfigurationDBRef();
            for(String prop : props) {
                String[] fields = prop.split("=");
                if("initialSize".equals(fields[0])) {
                    pps.put(fields[0], Integer.parseInt(fields[1]));
                } else {
                    pps.put(fields[0], fields[1]);
                }
            }

            configDB.setDataSourceFactory(pps, dbInfo.getSourceFactory());
            //set cache reference type
            ConfigurationCacheReferenceType cacheReferenceType = null;
            if("WEAK".equals(dbInfo.getCacheReferenceType())) {
                cacheReferenceType = ConfigurationCacheReferenceType.WEAK;
            } else if("SOFT".equals(dbInfo.getCacheReferenceType())) {
                cacheReferenceType = ConfigurationCacheReferenceType.SOFT;
            } else {
                cacheReferenceType = ConfigurationCacheReferenceType.HARD;
            }
            configDB.setExpiryTimeCache(dbInfo.getMaxAgeSeconds(), dbInfo.getPurgeIntervalSeconds(), cacheReferenceType);

            //set connection lifecycle
            ConfigurationDBRef.ConnectionLifecycleEnum connectionLifecycle = null;
            if("RETAIN".equals(dbInfo.getConnectionLifecycle())) {
                connectionLifecycle = ConfigurationDBRef.ConnectionLifecycleEnum.RETAIN;
            } else if("POOLED".equals(dbInfo.getConnectionLifecycle())) {
                connectionLifecycle = ConfigurationDBRef.ConnectionLifecycleEnum.POOLED;
            }
            configDB.setConnectionLifecycleEnum(connectionLifecycle);
            this.configuration.addDatabaseReference(dbInfo.getDbName(), configDB);

        }

        return this;
    }

    public AlarmEPLManager addMethods(Configuration configuration) {
        this.allMethods = DBUtils.qetAllMethods();
        Collection<Method> methods = this.allMethods.values();
        for(Method method : methods) {
            ConfigurationMethodRef methodRef = new ConfigurationMethodRef();
            ConfigurationCacheReferenceType cacheReferenceType = null;
            if("WEAK".equals(method.getCacheReferenceType())) {
                cacheReferenceType = ConfigurationCacheReferenceType.WEAK;
            } else if("SOFT".equals(method.getCacheReferenceType())) {
                cacheReferenceType = ConfigurationCacheReferenceType.SOFT;
            } else {
                cacheReferenceType = ConfigurationCacheReferenceType.HARD;
            }
            methodRef.setExpiryTimeCache(method.getMaxAgeSeconds(), method.getPurgeIntervalSeconds(), cacheReferenceType);
            Class clazz = null;
            try {
                clazz = Class.forName(method.getMethodName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            if(clazz == null) continue;
            this.configuration.addMethodRef(clazz, methodRef);
            this.configuration.addImport(method.getMethodName());
        }
        return this;
    }


    public AlarmEPLManager addEpls() {
        this.allEpls = DBUtils.qetAllAlarmEpls();
        Collection<AlarmEpl> epls = allEpls.values();
        EPStatement state = null;
        String eplSql = null;
        for(AlarmEpl epl : epls) {
            eplSql = epl.getEplSql();
            state = this.admin.createEPL(eplSql);
            state.addListener(new AlertListener(epl.getAlarmEplId(), epl.getEplId(), epl.getEplName(), epl.getAlarm()));
            if(state != null) eplStateMap.put(epl.getAlarmEplId(), state);
        }
        return this;
    }

    public AlarmEPLManager start() {
        this.runtime = epService.getEPRuntime();
        return this;
    }

    public Schema getSchema(int key) {
        return allSchema.get(key);
    }

    public EPRuntime getEPRuntime() {
        return runtime;
    }

    public void createPath(){
        try {
            //检测父节点是否存在
            if(curator.checkExists().forPath("/alarm") == null ){
                //父节点不存在：先创建父节点再创建子节点
                curator.create().forPath("/alarm");
                curator.create().forPath("/alarm/rule", "".getBytes());
            }else{
                //父节点存在：创建子节点
                if(curator.checkExists().forPath("/alarm/rule") == null ){
                    curator.create().forPath("/alarm/rule", "".getBytes());
                } else {
                    curator.setData().forPath("/alarm/rule", "".getBytes());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void recreateEpl(int eplId, AlarmEpl epl) {
        EPStatement epStatement = eplStateMap.get(eplId);
        if(epStatement != null) {
            epStatement.removeAllListeners();
            epStatement.destroy();
        }
        String eplSql = epl.getEplSql();
        epStatement = this.admin.createEPL(eplSql);
        epStatement.addListener(new AlertListener(epl.getAlarmEplId(), epl.getEplId(), epl.getEplName(), epl.getAlarm()));
        if(epStatement != null) eplStateMap.put(epl.getAlarmEplId(), epStatement);
        LOG.info("Recreate EPl : " + eplSql);
    }


    public void recreateSchema(int eplId, Schema schema) {
        EPStatement schemaState = schemaStateMap.get(eplId);
        if(schemaState != null) schemaState.destroy();
        String schemaSql = schema.getCreateSchemaSql();
        schemaState = this.admin.createEPL(schemaSql);
        if(schemaState != null) schemaStateMap.put(schema.getId(), schemaState);
        LOG.info("Recreate Schema  : " + schemaSql);
    }

    public void recreateMethod(Method method) {
        Method md = this.allMethods.get(method.getMethodName());
        if(md != null) this.configuration.removeImport(md.getMethodName());
        ConfigurationMethodRef methodRef = new ConfigurationMethodRef();
        ConfigurationCacheReferenceType cacheReferenceType = null;
        if("WEAK".equals(method.getCacheReferenceType())) {
            cacheReferenceType = ConfigurationCacheReferenceType.WEAK;
        } else if("SOFT".equals(method.getCacheReferenceType())) {
            cacheReferenceType = ConfigurationCacheReferenceType.SOFT;
        } else {
            cacheReferenceType = ConfigurationCacheReferenceType.HARD;
        }
        methodRef.setExpiryTimeCache(method.getMaxAgeSeconds(), method.getPurgeIntervalSeconds(), cacheReferenceType);
        Class clazz = null;
        try {
            clazz = Class.forName(method.getMethodName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.configuration.addMethodRef(clazz, methodRef);
        this.configuration.addImport(method.getMethodName());
        LOG.info("Recreate Method  : " + method.getMethodName());
    }

    public void recreateDB(DBInfo dbInfo) {
        DBInfo db = this.allDbs.get(dbInfo.getDbName());
        if(db != null) return;
        Properties pps = new Properties();
        String[] props = dbInfo.getProps();
        ConfigurationDBRef configDB = new ConfigurationDBRef();
        for(String prop : props) {
            String[] fields = prop.split("=");
            if("initialSize".equals(fields[0])) {
                pps.put(fields[0], Integer.parseInt(fields[1]));
            } else {
                pps.put(fields[0], fields[1]);
            }
        }

        configDB.setDataSourceFactory(pps, dbInfo.getSourceFactory());
        //set cache reference type
        ConfigurationCacheReferenceType cacheReferenceType = null;
        if("WEAK".equals(dbInfo.getCacheReferenceType())) {
            cacheReferenceType = ConfigurationCacheReferenceType.WEAK;
        } else if("SOFT".equals(dbInfo.getCacheReferenceType())) {
            cacheReferenceType = ConfigurationCacheReferenceType.SOFT;
        } else {
            cacheReferenceType = ConfigurationCacheReferenceType.HARD;
        }
        configDB.setExpiryTimeCache(dbInfo.getMaxAgeSeconds(), dbInfo.getPurgeIntervalSeconds(), cacheReferenceType);

        //set connection lifecycle
        ConfigurationDBRef.ConnectionLifecycleEnum connectionLifecycle = null;
        if("RETAIN".equals(dbInfo.getConnectionLifecycle())) {
            connectionLifecycle = ConfigurationDBRef.ConnectionLifecycleEnum.RETAIN;
        } else if("POOLED".equals(dbInfo.getConnectionLifecycle())) {
            connectionLifecycle = ConfigurationDBRef.ConnectionLifecycleEnum.POOLED;
        }
        configDB.setConnectionLifecycleEnum(connectionLifecycle);
        this.configuration.addDatabaseReference(dbInfo.getDbName(), configDB);
        LOG.info("Recreate Data Source  : " + dbInfo.getDbName());
    }


    public void refresh(List<Map<Object, Object>> zkDataList) {

        isUpdate = true;
        try {

            for (Map<Object, Object> zkData : zkDataList) {

                if ("method".equals(zkData.get("type").toString())) {
                    if(zkData.containsKey("action") && "remove".equals(zkData.get("action"))) {
                        destory(Integer.parseInt(zkData.get("id").toString()), "method");
                    } else if(zkData.containsKey("action") && ("update".equals(zkData.get("action")) || "add".equals(zkData.get("action")))) {
                        Method method = new Method(Integer.parseInt(zkData.get("id").toString()), zkData.get("methodName").toString(), zkData.get("cacheReferenceType").toString(), zkData.get("dbDescribe").toString(), Integer.parseInt(zkData.get("maxAgeSeconds").toString()), Integer.parseInt(zkData.get("purgeIntervalSeconds").toString()));
                        recreateMethod(method);
                    }
                } else if ("epl".equals(zkData.get("type").toString())) {
                    if(zkData.containsKey("action") && "remove".equals(zkData.get("action"))) {
                        destory(Integer.parseInt(zkData.get("id").toString()), "epl");
                    } else if(zkData.containsKey("action") && ("update".equals(zkData.get("action")) || "add".equals(zkData.get("action")))) {
                        Alarm alarm = new Alarm(Integer.parseInt(zkData.get("alarmEplId").toString()), Integer.parseInt(zkData.get("eplId").toString()), Integer.parseInt(zkData.get("alarmType").toString()), zkData.get("phone").toString(), zkData.get("email").toString(), zkData.get("alarmTemplate").toString());
                        AlarmEpl epl = new AlarmEpl(Integer.parseInt(zkData.get("alarmEplId").toString()), Integer.parseInt(zkData.get("eplId").toString()), zkData.get("eplName").toString(), zkData.get("eplSql").toString(), alarm);
                        recreateEpl(epl.getAlarmEplId(), epl);
                    }
                } else if ("db".equals(zkData.get("type").toString())) {
                    if(zkData.containsKey("action") && "remove".equals(zkData.get("action"))) {
                        destory(Integer.parseInt(zkData.get("id").toString()), "db");
                    } else if(zkData.containsKey("action") && ("update".equals(zkData.get("action")) || "add".equals(zkData.get("action")))) {
                        DBInfo dbInfo = new DBInfo(Integer.parseInt(zkData.get("id").toString()), zkData.get("props").toString().split(","), zkData.get("dbName").toString(), zkData.get("connectionLifecycle").toString(), zkData.get("cacheReferenceType").toString(), zkData.get("dbDescribe").toString(), zkData.get("sourceFactory").toString(), Integer.parseInt(zkData.get("maxAgeSeconds").toString()), Integer.parseInt(zkData.get("purgeIntervalSeconds").toString()));
                        recreateDB(dbInfo);
                    }
                } else if ("schema".equals(zkData.get("type").toString())) {
                    if(zkData.containsKey("action") && "remove".equals(zkData.get("action"))) {
                        destory(Integer.parseInt(zkData.get("id").toString()), "schema");
                    } else if(zkData.containsKey("action") && ("update".equals(zkData.get("action")) || "add".equals(zkData.get("action")))) {
                        Schema schema = new Schema(Integer.parseInt(zkData.get("id").toString()), zkData.get("schemaName").toString(), zkData.get("createSchemaSql").toString());
                        recreateSchema(schema.getId(), schema);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            isUpdate = false;
            this.latch.countDown();
        }

    }

    public void destory(Object id, String type) {
        if ("method".equals(type) && id != null) {
            Method md = this.allMethods.get(id);
            if(md != null) this.configuration.removeImport(md.getMethodName());
        } else if ("epl".equals(type)) {
            if(eplStateMap.containsKey(id) && !eplStateMap.get(id).isDestroyed()) {
                eplStateMap.get(id).destroy();
                eplStateMap.remove(id);
            }
        } else if ("db".equals(type)) {
            this.configuration.getDatabaseReferences().remove(id);
        } else if ("schema".equals(type)) {
            if(schemaStateMap.containsKey(id) && !schemaStateMap.get(id).isDestroyed()) {
                schemaStateMap.get(id).destroy();
                schemaStateMap.remove(id);
            }
        }
    }


    public boolean isUpdate() {
        return this.isUpdate;
    }

    public CountDownLatch getCountDownLatch() {
        return this.latch;
    }
}
