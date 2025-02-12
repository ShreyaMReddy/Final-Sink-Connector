package com.example;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.*;

public class ElasticsearchConnector implements IConnectorSink {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConnector.class);
    private static final long serialVersionUID = 1L;
    private final String indexName;
    private final String sourceTopic;

    public ElasticsearchConnector(String indexName, String sourceTopic) {
        this.indexName = indexName;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void setSinkStream(StreamExecutionEnvironment env, Properties config, SingleOutputStreamOperator<String> dataStream) {
        LOG.info("Elasticsearch sink stream configured for index: {}", indexName);
        dataStream.process(getSinkFunction(
            new ConnectorContext("elasticsearch", UUID.randomUUID().toString(), sourceTopic),
            config
        )).name("elasticsearch-sink");
    }

    @Override
    public SinkConnectorFunction getSinkFunction(ConnectorContext connectorCtx, Properties config) {
        try {
            return new ElasticsearchSinkFunction(connectorCtx, config, indexName, sourceTopic);
        } catch (Exception e) {
            LOG.error("Failed to create ElasticsearchSinkFunction: {}", e.getMessage());
            throw new RuntimeException("Failed to create sink function", e);
        }
    }

    public static void createIndexWithMapping(String indexName, String mappingType, Properties config) {
        RestClient restClient = null;
        try {
            restClient = RestClient.builder(
                HttpHost.create(config.getProperty("elasticsearch.hosts", "http://elasticsearch:9200"))
            ).build();

            ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
            );

            ElasticsearchClient client = new ElasticsearchClient(transport);

            // Check if index exists
            boolean indexExists = client.indices().exists(e -> e.index(indexName)).value();
            if (indexExists) {
                LOG.info("Index {} already exists, skipping creation", indexName);
                return;
            }

            // Load mapping from resources
            String mappingPath = String.format("mappings/%s_mapping.json", indexName);
            InputStream mappingStream = ElasticsearchConnector.class.getClassLoader().getResourceAsStream(mappingPath);
            
            if (mappingStream == null) {
                LOG.warn("No mapping file found at {}, creating index with default mapping", mappingPath);
                CreateIndexResponse response = client.indices().create(c -> c.index(indexName));
                LOG.info("Created index {} with default mapping: {}", indexName, response.acknowledged());
                return;
            }

            // Read the mapping JSON
            ObjectMapper mapper = new ObjectMapper();
            JsonNode mappingJson = mapper.readTree(mappingStream);
            String mappingStr = mappingJson.toString();

            // Create index with mapping
            try (JsonReader jsonReader = Json.createReader(new StringReader(mappingStr))) {
                JsonObject jsonObject = jsonReader.readObject();
                
                CreateIndexResponse response = client.indices().create(c -> c
                    .index(indexName)
                    .mappings(m -> m.withJson(new StringReader(mappingStr)))
                );

                LOG.info("Created index {} with mapping: {}", indexName, response.acknowledged());
            }

        } catch (IOException e) {
            LOG.error("Failed to create index with mapping: {}", e.getMessage());
            throw new RuntimeException("Failed to create index", e);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                } catch (IOException e) {
                    LOG.error("Error closing REST client: {}", e.getMessage());
                }
            }
        }
    }
}
