package com.madcoder;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final String TOPIC_NAME = "test";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public Application(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            kafkaTemplate.send(TOPIC_NAME, "test" + i);
        }
        Thread.sleep(3000);
        System.exit(0);
    }
}
