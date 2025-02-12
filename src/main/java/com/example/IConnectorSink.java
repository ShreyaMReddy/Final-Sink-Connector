package com.example;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.Serializable;
import java.util.Properties;

public interface IConnectorSink extends Serializable {
    void setSinkStream(StreamExecutionEnvironment env, Properties config, SingleOutputStreamOperator<String> dataStream);
    SinkConnectorFunction getSinkFunction(ConnectorContext connectorCtx, Properties config);
}
