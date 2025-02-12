package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReceivedMapper implements MapFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageReceivedMapper.class);
    private final boolean logMessages;

    public MessageReceivedMapper(boolean logMessages) {
        this.logMessages = logMessages;
    }

    @Override
    public String map(String value) {
        if (logMessages) {
            LOG.info("Received message: {}", value);
        }
        return value;
    }
}
