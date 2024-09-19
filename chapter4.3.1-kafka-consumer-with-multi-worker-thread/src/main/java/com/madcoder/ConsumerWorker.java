package com.madcoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWorker implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithMultiWorkerThread.class);
    private final String recordValue;

    public ConsumerWorker(String recordValue) {
        this.recordValue = recordValue;
    }

    @Override
    public void run() {
        logger.info("thread:{}\trecord: {}", Thread.currentThread().getName(), recordValue);
    }
}
