package com.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import scala.collection.Seq;

import java.util.Properties;


public class TopicCreater {
    private static final int SESSION_TIME_OUT = 15 * 1000;
    private static final int CONNECTION_TIME_OUT = 10 * 1000;

    private final String kafkaZK;

    public TopicCreater(String kafkaZK) {
        this.kafkaZK = kafkaZK;
    }

    public boolean isTopicNotExists(String topicName) {
        boolean isNotExists = true;
        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(kafkaZK, SESSION_TIME_OUT, CONNECTION_TIME_OUT, false);
            Seq<String> topics = zkUtils.getAllTopics();
            if (topics.contains(topicName))
                isNotExists = false;
        } catch (RuntimeException e) {
            e.printStackTrace();
        } finally {
            if (null != zkUtils)
                zkUtils.close();
        }
        return isNotExists;
    }

    public void createTopic(String topicName, int partitions, int replication) {
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(kafkaZK, SESSION_TIME_OUT, CONNECTION_TIME_OUT, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtil = new ZkUtils(zkClient, new ZkConnection(kafkaZK), false);
            AdminUtils.createTopic(zkUtil, topicName, partitions, replication, new Properties(), RackAwareMode.Enforced$.MODULE$);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != zkClient)
                zkClient.close();
        }
    }
}
