package com.madcoder;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            logger.error(e.getMessage(), e);
        } else {
            logger.info("timestamp of metadata: {}", recordMetadata.timestamp());

            logger.info("topic: {}", recordMetadata.topic());
            logger.info("partition: {}", recordMetadata.partition());
            logger.info("offset: {}", recordMetadata.offset());
        }
    }
}
