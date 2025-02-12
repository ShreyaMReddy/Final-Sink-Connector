package com.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;

public class MessageProcessingMapper implements IConnectorSink {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MessageProcessingMapper.class);
    private final String mappingType;
    private final String indexName;
    private final String sourceTopic;

    public MessageProcessingMapper(String mappingType, String indexName, String sourceTopic) {
        this.mappingType = mappingType;
        this.indexName = indexName;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void setSinkStream(StreamExecutionEnvironment env, Properties config, SingleOutputStreamOperator<String> dataStream) {
        // Apply the processing function to the data stream
        SingleOutputStreamOperator<String> processedStream = dataStream.map(new MessageProcessor(mappingType));
        
        // Get the sink function and add it to the stream
        ConnectorContext connectorCtx = new ConnectorContext("elasticsearch", indexName, sourceTopic);
        SinkConnectorFunction sinkFunction = getSinkFunction(connectorCtx, config);
        processedStream.process(sinkFunction);
    }

    @Override
    public SinkConnectorFunction getSinkFunction(ConnectorContext connectorCtx, Properties config) {
        return new ElasticsearchSinkFunction(connectorCtx, config, indexName, sourceTopic);
    }

    private static class MessageProcessor extends RichMapFunction<String, String> {
        private static final long serialVersionUID = 1L;
        private transient Counter processedCounter;
        private transient Counter errorCounter;
        private transient ObjectMapper objectMapper;
        private final String mappingType;

        public MessageProcessor(String mappingType) {
            this.mappingType = mappingType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            processedCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("processed-messages");
            errorCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("error-messages");
            objectMapper = new ObjectMapper();
        }

        @Override
        public String map(String value) throws Exception {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                
                if (!jsonNode.isObject()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping non-object message: {}", value);
                    }
                    errorCounter.inc();
                    return null;
                }

                ObjectNode enrichedNode = (ObjectNode) jsonNode;
                enrichedNode.put("processed_time", System.currentTimeMillis());
                enrichedNode.put("_mapping_type", mappingType);

                String processedValue = objectMapper.writeValueAsString(enrichedNode);
                processedCounter.inc();
                
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Processed message: {}", processedValue);
                }

                return processedValue;
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping invalid message: {}, Error: {}", value, e.getMessage());
                }
                errorCounter.inc();
                return null;
            }
        }
    }
}
