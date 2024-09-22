package com.madcoder;

import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final String TOPIC_NAME = "test";
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final KafkaTemplate<String, String> customKafkaTemplate;

    public Application(KafkaTemplate<String, String> customKafkaTemplate) {
        this.customKafkaTemplate = customKafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws InterruptedException {
        String message = "test";
        CompletableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Sent message=[ {} ] with offset=[ {} ]", message, result.getRecordMetadata().offset());
            } else {
                logger.info("Unable to send message=[ {} ] due to : {}", message, ex.getMessage());
            }
        });

        Thread.sleep(3000);
        System.exit(0);
    }
}
