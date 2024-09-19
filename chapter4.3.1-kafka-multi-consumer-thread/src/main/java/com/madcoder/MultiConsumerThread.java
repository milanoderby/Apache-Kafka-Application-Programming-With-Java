package com.madcoder;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MultiConsumerThread {
    private static final String TOPIC_NAME = "test2";
    private static final String BOOTSTRAP_SERVERS = "madcoder-kafka-server:9092";
    private static final String GROUP_ID = "test-group";
    private static final int COUNT_OF_CONSUMER = 3;

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < COUNT_OF_CONSUMER; i++) {
            ConsumerWorker worker = new ConsumerWorker(configs, TOPIC_NAME, i);
            executorService.execute(worker);
        }
    }
}
