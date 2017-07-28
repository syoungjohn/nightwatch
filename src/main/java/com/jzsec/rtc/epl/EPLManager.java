package com.jzsec.rtc.epl;

import com.espertech.esper.client.*;
import com.jzsec.rtc.bean.*;
import com.jzsec.rtc.lookup.FundAssetsLookUp;
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
public class EPLManager implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(EPLManager.class);
    private EPAdministrator admin = null;
    private Map<Integer, List<Schema>>  allSchema = null;
    private Map<Integer, Epl> allEpls = null;
    private Map<String, Method> allMethods = null;
    private Map<String, DBInfo> allDbs = null;
    private EPRuntime runtime = null;
    private EPServiceProvider epService = null;
    private Map<Integer, List<EPStatement>> schemaStateMap = new HashMap<Integer, List<EPStatement>>();
    private Map<Integer, EPStatement> eplStateMap = new HashMap<Integer, EPStatement>();
    private CuratorFramework curator;
    private Map conf = null;
    private com.espertech.esper.client.Configuration configuration = null;
    private CountDownLatch latch;
    private boolean isUpdate = false;
    private Map<Integer, Set<String>> groupKeyMap = new HashMap<Integer, Set<String>>();
    private OutputCollector collector;
    public EPLManager(Map conf, OutputCollector collector, CountDownLatch latch) {

        if(collector != null) this.collector = collector;
        this.conf = conf;
        this.latch = latch;
        this.configuration = new com.espertech.esper.client.Configuration();
        this.configuration.getEngineDefaults().getThreading().setInsertIntoDispatchPreserveOrder(false);
        this.addMethods(configuration);
        this.addDbs();


        this.epService = EPServiceProviderManager.getDefaultProvider(configuration);
        this.admin = epService.getEPAdministrator();
        this.addSchemas(null);
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
        final PathChildrenCache pathChildrenCache = new PathChildrenCache(curator, "/risk", true);
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
                                    if(!zkData.containsKey("type")) {
                                        String childPath = childData.getPath();
                                        zkData.put("type", childPath.substring(childPath.lastIndexOf("/") + 1));
                                    }
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

    public EPLManager addSchemas(Integer schemaId) {
        this.allSchema = DBUtils.qetAllSchema(schemaId);
        Collection<List<Schema>> schemas = allSchema.values();
        for(List<Schema> schemaList : schemas) {
            for(Schema schema : schemaList) {
                if(schemaStateMap.containsKey(schema.getId())) {
                    schemaStateMap.get(schema.getId()).add(this.admin.createEPL(schema.getCreateSchemaSql()));
                } else {
                    List<EPStatement> epsList = new ArrayList<EPStatement>();
                    epsList.add(this.admin.createEPL(schema.getCreateSchemaSql()));
                    schemaStateMap.put(schema.getId(), epsList);
                }
                if(groupKeyMap.containsKey(schema.getId())) {
                    groupKeyMap.get(schema.getId()).add(schema.getFieldGroupKey());
                } else {
                    Set<String> groupKeySet = new HashSet<String>();
                    groupKeySet.add(schema.getFieldGroupKey());
                    groupKeyMap.put(schema.getId(), groupKeySet);
                }
                LOG.info("Add Data Schema : " + schema.getCreateSchemaSql());
            }
        }
        return this;
    }

    public EPLManager addDbs() {
        this.allDbs = DBUtils.qetAllDBInfos();
        Collection<DBInfo> dbInfos = this.allDbs.values();
        for (DBInfo dbInfo : dbInfos) {
            Properties pps = new Properties();
            String[] props = dbInfo.getProps();
            ConfigurationDBRef configDB = new ConfigurationDBRef();
            for(String prop : props) {
                String[] fields = prop.split("=");
                if(fields.length != 2) continue;
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
            LOG.info("Add Data Source : " + dbInfo.getDbName());

        }

        return this;
    }

    public EPLManager addMethods(com.espertech.esper.client.Configuration configuration) {
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
            LOG.info("Add customer method : " + method.getMethodName());
            this.configuration.addMethodRef(clazz, methodRef);
            this.configuration.addImport(method.getMethodName());
        }
        return this;
    }


    public EPLManager addEpls() {
        this.allEpls = DBUtils.qetAllEpls(null);
        Collection<Epl> epls = allEpls.values();
        EPStatement state = null;
        String eplSql = null;
        for(Epl epl : epls) {
            eplSql = epl.getEplSql();
            String[] eplSqls = eplSql.trim().split(";");
            for(int i = 0; i < eplSqls.length; i++) {

                if(!StringUtils.isBlank(eplSqls[i])) {
                    if(i == eplSqls.length - 1) {
                        LOG.info("Add statistic epl : " + eplSql + ", name: "  + epl.getEplName());
                        state = this.admin.createEPL(eplSqls[i].trim());
                        state.addListener(new StatisticListener(epl, collector));
                        LOG.info("Add statistic epl : " + eplSql + ", name: "  + epl.getEplName());
                        if(state != null) eplStateMap.put(epl.getId(), state);
                    } else {
                        admin.createEPL(eplSqls[i].trim());
                    }
                }

            }
        }
        return this;
    }

    public EPLManager start() {
        this.runtime = epService.getEPRuntime();
        return this;
    }

    public List<Schema> getSchema(int key) {
        return allSchema.get(key);
    }

    public EPRuntime getEPRuntime() {
        return runtime;
    }

    public void createPath(){
        try {
            //检测父节点是否存在
            if(curator.checkExists().forPath("/risk") == null ){
                //父节点不存在：先创建父节点再创建子节点
                curator.create().forPath("/risk");
                curator.create().forPath("/risk/rule", "".getBytes());
            }else{
                //父节点存在：创建子节点
                if(curator.checkExists().forPath("/risk/rule") == null ){
                    curator.create().forPath("/risk/rule", "".getBytes());
                } else {
                    curator.setData().forPath("/risk/rule", "".getBytes());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void recreateEpl(int eplId, Epl epl) {
        EPStatement epStatement = eplStateMap.get(eplId);
        if(epStatement != null && !epStatement.isDestroyed()) {
            epStatement.removeAllListeners();
            epStatement.destroy();

        }
        String eplSql = epl.getEplSql();
        String[] eplSqls = eplSql.trim().split(";");

        for(int i = 0; i < eplSqls.length; i++) {

            if(!StringUtils.isBlank(eplSqls[i])) {
                if(i == eplSqls.length - 1) {
                    epStatement = this.admin.createEPL(eplSqls[i].trim());
                    LOG.info("Add statistic epl : " + eplSql);
                    if(epStatement != null) eplStateMap.put(epl.getId(), epStatement);
                } else {
                    admin.createEPL(eplSqls[i].trim());
                }
            }

        }
        epStatement.addListener(new StatisticListener(epl, collector));
        LOG.info("Recreate statistic epl : " + eplSql);
    }


    public void recreateSchema(int schemaId, Schema schema) {
        List<EPStatement> schemaStates = schemaStateMap.get(schemaId);
        for(EPStatement schemaState : schemaStates) {
            if(schemaState != null && !schemaState.isDestroyed()) schemaState.destroy();
            groupKeyMap.get(schema.getId()).clear();
        }
        addSchemas(schema.getId());
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
        LOG.info("Recreate Method : " + method.getMethodName());
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
        LOG.info("Recreate Data Source : " + dbInfo.getDbName());
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
                        Map<Integer, Epl> epls = DBUtils.qetAllEpls(Integer.parseInt(zkData.get("id").toString()));
                        if(epls.containsKey(Integer.parseInt(zkData.get("id").toString())) && epls.get(Integer.parseInt(zkData.get("id").toString())) != null) {
                            Epl epl = epls.get(Integer.parseInt(zkData.get("id").toString()));
                            recreateEpl(epl.getId(), epl);
                            epl = null;
                        }
                        epls = null;
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
                        Schema schema = new Schema();
                        schema.setId(Integer.parseInt(zkData.get("id").toString()));
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
            LOG.info("Remove Method : " + md.getMethodName());
        } else if ("epl".equals(type)) {
            if(eplStateMap.containsKey(id) && !eplStateMap.get(id).isDestroyed()) {
                eplStateMap.get(id).destroy();
                LOG.info("Destroy Method : " + eplStateMap.get(id).getText());
                eplStateMap.remove(id);
            }
        } else if ("db".equals(type)) {
            if(this.configuration.getDatabaseReferences().containsKey(id)) {
                this.configuration.getDatabaseReferences().remove(id);
                LOG.info("Remove Data Source : " + this.configuration.getDatabaseReferences().get(id));
            }
        } else if ("schema".equals(type)) {
            if(schemaStateMap.containsKey(id)) {
                List<EPStatement> epStatementList = schemaStateMap.get(id);
                for(EPStatement ePStatement: epStatementList) {
                    if(!ePStatement.isDestroyed()) {
                        LOG.info("Destroy Data Schema : " + schemaStateMap.get(id));
                        ePStatement.destroy();
                    }
                }
            }
            schemaStateMap.remove(id);
        }
    }


    public boolean isUpdate() {
        return this.isUpdate;
    }

    public CountDownLatch getCountDownLatch() {
        return this.latch;
    }

    public Set<String> getGroupKeys(Integer schemaId) {
        return groupKeyMap.get(schemaId);
    }
}
