package com.madcoder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWorker implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private final Properties configs;
    private final String topic;
    private final String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties configs, String topic, int number) {
        this.configs = configs;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("thread: {}\trecord: {}", threadName, record);
            }
        }
    }
}
