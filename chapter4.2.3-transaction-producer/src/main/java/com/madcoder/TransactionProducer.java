package com.madcoder;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "madcoder-kafka-server:9092";

    public static void main(String[] args) throws InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-producer-id");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> transactionProducer = new KafkaProducer<>(configs);

        transactionProducer.initTransactions();
        transactionProducer.beginTransaction();

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "First Message");
        transactionProducer.send(record);
        logger.info("{}", record);
        transactionProducer.flush();

        Thread.sleep(5000);

        record = new ProducerRecord<>(TOPIC_NAME, "Second Message");
        transactionProducer.send(record);
        logger.info("{}", record);
        transactionProducer.flush();

        Thread.sleep(5000);

        record = new ProducerRecord<>(TOPIC_NAME, "Third Message");
        transactionProducer.send(record);
        logger.info("{}", record);
        transactionProducer.flush();

        Thread.sleep(5000);

        transactionProducer.commitTransaction();
        // producer.abortTransaction();

        transactionProducer.close();
    }
}