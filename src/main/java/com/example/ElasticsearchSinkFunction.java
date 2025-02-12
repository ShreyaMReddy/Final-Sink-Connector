package com.example;

import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ElasticsearchSinkFunction extends SinkConnectorFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkFunction.class);
    private static final long serialVersionUID = 1L;
    private final String indexName;
    private final String sourceTopic;
    private transient ElasticsearchClient esClient;
    private transient ObjectMapper objectMapper;
    private transient List<BulkOperation> bulkOperations;
    private transient long lastBulkTime;
    private final int batchSize;
    private final long flushIntervalMs;

    public ElasticsearchSinkFunction(ConnectorContext connectorCtx, Properties config, String indexName, String sourceTopic) {
        super(connectorCtx, config);
        this.indexName = indexName;
        this.sourceTopic = sourceTopic;
        this.batchSize = Integer.parseInt(config.getProperty("elasticsearch.batch.size", "1000"));
        this.flushIntervalMs = Long.parseLong(config.getProperty("elasticsearch.flush.interval.ms", "5000"));
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        RestClient restClient = RestClient.builder(
            HttpHost.create(config.getProperty("elasticsearch.hosts", "http://elasticsearch:9200"))
        ).build();

        ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper()
        );

        esClient = new ElasticsearchClient(transport);
        objectMapper = new ObjectMapper();
        bulkOperations = new ArrayList<>();
        lastBulkTime = System.currentTimeMillis();
    }

    @Override
    public void processElement(String value, Context context, Collector<String> out) throws Exception {
        LOG.debug("Processing message for Elasticsearch: {}", value);
        if (value == null) {
            return;  // Skip null values from upstream
        }

        try {
            JsonNode jsonNode = objectMapper.readTree(value);
            
            // Add to bulk operations
            bulkOperations.add(BulkOperation.of(op -> op
                .index(idx -> idx
                    .index(indexName)
                    .document(jsonNode)
                )
            ));

            // Check if we should flush based on batch size or time
            boolean shouldFlushSize = bulkOperations.size() >= batchSize;
            boolean shouldFlushTime = (System.currentTimeMillis() - lastBulkTime) >= flushIntervalMs;

            if (shouldFlushSize || shouldFlushTime) {
                LOG.debug("Triggering flush with {} documents", bulkOperations.size());
                flush();
                lastBulkTime = System.currentTimeMillis();
            }

            out.collect(value);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skipping document due to error: {}", e.getMessage());
            }
        }
    }

    private void flush() {
        if (bulkOperations.isEmpty()) {
            return;
        }

        try {
            BulkRequest.Builder bulkBuilder = new BulkRequest.Builder();
            bulkOperations.forEach(bulkBuilder::operations);
            
            BulkResponse response = esClient.bulk(bulkBuilder.build());
            
            if (response.errors()) {
                // Log failed operations but continue processing
                response.items().forEach(item -> {
                    if (item.error() != null) {
                        LOG.warn("Failed to index document: {}", item.error().reason());
                    }
                });
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Bulk indexed {} documents to {} in {}ms", 
                    bulkOperations.size(), 
                    indexName,
                    response.took());
            }
        } catch (Exception e) {
            LOG.error("Failed to execute bulk request: {}", e.getMessage());
        } finally {
            bulkOperations.clear();
            lastBulkTime = System.currentTimeMillis();
        }
    }

    @Override
    public void close() throws Exception {
        // Flush any remaining documents before closing
        flush();
        
        if (esClient != null) {
            esClient._transport().close();
        }
    }
}
