package com.example;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serializable;
import java.util.Properties;

public abstract class SinkConnectorFunction extends ProcessFunction<String, String> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final ConnectorContext connectorContext;
    protected final Properties config;

    public SinkConnectorFunction(ConnectorContext connectorContext, Properties config) {
        this.connectorContext = connectorContext;
        this.config = config;
    }

    @Override
    public abstract void processElement(String value, Context context, Collector<String> out) throws Exception;
}
