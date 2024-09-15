package com.madcoder;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);
    private static final String BOOTSTRAP_SERVERS = "madcoder-kafka-server:9092";

    public static void main(String[] args) throws Exception {
        Properties configs = new Properties();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        AdminClient admin = AdminClient.create(configs);

        logger.info("== Get broker information");
        for (Node node : admin.describeCluster().nodes().get()) {
            logger.info("Node: {}", node);
            ConfigResource cr = new ConfigResource(Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry -> {
                    logger.info("{} = {}", configEntry.name(), configEntry.value());
                });
            });
        }

        logger.info("== Get default num.partitions");
        for (Node node : admin.describeCluster().nodes().get()) {
            ConfigResource cr = new ConfigResource(Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            Config config = describeConfigs.all().get().get(cr);
            Optional<ConfigEntry> optionalConfigEntry = config.entries().stream().filter(v -> v.name().equals("num.partitions")).findFirst();
            ConfigEntry numPartitionConfig = optionalConfigEntry.orElseThrow(Exception::new);
            logger.info("{}", numPartitionConfig.value());
        }

        logger.info("== Topic list");
        for (TopicListing topicListing : admin.listTopics().listings().get()) {
            logger.info("{}", topicListing.toString());
        }

        logger.info("== test topic information");
        Map<String, TopicDescription> topicInformation = admin.describeTopics(Collections.singletonList("test")).all().get();
        logger.info("{}", topicInformation);

        logger.info("== Consumer group list");
        ListConsumerGroupsResult listConsumerGroups = admin.listConsumerGroups();
        listConsumerGroups.all().get().forEach(v -> {
            logger.info("{}", v);
        });

        admin.close();
    }
}