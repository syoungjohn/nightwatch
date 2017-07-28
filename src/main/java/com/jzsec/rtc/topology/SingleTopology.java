package com.jzsec.rtc.topology;

import com.jzsec.rtc.bolt.AlarmBolt;
import com.jzsec.rtc.bolt.ExtractBolt;
import com.jzsec.rtc.bolt.StatisticBolt;
import com.jzsec.rtc.config.Configuration;
import kafka.tools.ConsoleConsumer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by caodaoxi on 16-4-5.
 */
public class SingleTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SingleTopology.class);
    private static Config config = new Config();

    public static void main(String[] args) throws Exception {

        Configuration.init("rtc_local.properties");
        TopologyBuilder builder = buildTopology();
        submitTopology(builder);
    }

    private static TopologyBuilder buildTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String topicName = Configuration.getConfig().getString("rtc.mq.spout.topic");
        String groupName = Configuration.getConfig().getString("rtc.mq.spout.group");
        BrokerHosts hosts = new ZkHosts(Configuration.getConfig().getString("rtc.zk.hosts"));
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/consumers", groupName);
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        spoutConfig.zkServers = Arrays.asList(Configuration.getConfig().getString("rtc.storm.zkServers").split(","));
        spoutConfig.zkPort = Configuration.getConfig().getInt("rtc.storm.zkPort");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("MQSpout", kafkaSpout, Configuration.getConfig().getInt("rtc.storm.spout.parallelismHint")).setNumTasks(Configuration.getConfig().getInt("rtc.storm.spout.task"));
        builder.setBolt("ExtractBolt", new ExtractBolt(), Configuration.getConfig().getInt("rtc.storm.extract.bolt.parallelismHint")).setNumTasks(Configuration.getConfig().getInt("rtc.storm.extract.bolt.task")).shuffleGrouping("MQSpout");
        builder.setBolt("Statistic", new StatisticBolt(), Configuration.getConfig().getInt("rtc.storm.statistic.bolt.parallelismHint")).setNumTasks(Configuration.getConfig().getInt("rtc.storm.statistic.bolt.task")).fieldsGrouping("ExtractBolt", new Fields(new String[]{"hashKeys"}));
//        builder.setBolt("Alarm", new AlarmBolt(), Configuration.getConfig().getInt("rtc.storm.alarm.bolt.parallelismHint")).setNumTasks(Configuration.getConfig().getInt("rtc.storm.alarm.bolt.task")).fieldsGrouping("Statistic", new Fields(new String[]{"EventName"}));
        return builder;
    }

    private static void submitTopology(TopologyBuilder builder) {
        try {
            LocalCluster cluster = new LocalCluster();
            config.put(Config.STORM_CLUSTER_MODE, "local");
            cluster.submitTopology(Configuration.getConfig().getString("topology.name"), config, builder.createTopology());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e.getCause());
        }
    }
}
