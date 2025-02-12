package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

public class FlinkJobMain {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobMain.class);
    private static final Properties CONFIG = loadConfig();

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: FlinkJobMain <kafka_topic> <mapping_type>");
            System.exit(1);
        }

        String topic = args[0];
        String mappingType = args[1];

        // Validate mapping type exists
        if (!isMappingTypeValid(mappingType)) {
            System.err.println("Error: Invalid mapping type '" + mappingType + "'");
            System.err.println("Available mapping types: " + getAvailableMappingTypes());
            System.exit(1);
        }

        String indexName = mappingType.toLowerCase();

        try {
            // Create Elasticsearch index with mapping before starting the job
            ElasticsearchConnector.createIndexWithMapping(indexName, mappingType, CONFIG);
            LOG.info("Created Elasticsearch index with mapping: {}", indexName);

            // Create Flink execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(Integer.parseInt(CONFIG.getProperty("flink.parallelism", "1")));

            // Configure Kafka source
            KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(CONFIG.getProperty("kafka.bootstrap.servers"))
                .setTopics(topic)
                .setGroupId(CONFIG.getProperty("kafka.consumer.group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

            // Create message processing mapper
            MessageProcessingMapper processingMapper = new MessageProcessingMapper(mappingType, indexName, topic);

            // Set up the processing pipeline
            SingleOutputStreamOperator<String> processedStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .name("kafka-source")
                .map(new MessageReceivedMapper(Boolean.parseBoolean(CONFIG.getProperty("logging.message.received", "false"))))
                .name("message-received-mapper");

            // Apply the processing mapper
            processingMapper.setSinkStream(env, CONFIG, processedStream);

            // Execute the job
            env.execute("Kafka to Elasticsearch Job");

        } catch (Exception e) {
            LOG.error("Error executing Flink job: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static Properties loadConfig() {
        Properties configProps = new Properties();
        
        try (InputStream input = FlinkJobMain.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new IOException("application.properties not found");
            }
            configProps.load(input);
            LOG.info("Loaded configuration from application.properties");
        } catch (Exception e) {
            LOG.error("Error loading properties file: {}", e.getMessage());
            throw new RuntimeException("Failed to load configuration", e);
        }

        return configProps;
    }

    private static boolean isMappingTypeValid(String mappingType) {
        String mappingFileName = mappingType + "_mapping.json";
        LOG.info("Checking for mapping file: {}", mappingFileName);
        
        try (InputStream input = FlinkJobMain.class.getClassLoader()
                .getResourceAsStream("mappings/" + mappingFileName)) {
            boolean exists = input != null;
            LOG.info("Mapping file {} exists: {}", mappingFileName, exists);
            return exists;
        } catch (IOException e) {
            LOG.error("Error checking mapping type: {}", e.getMessage());
            return false;
        }
    }

    private static String getAvailableMappingTypes() {
        Set<String> mappingTypes = new HashSet<>();
        try {
            ClassLoader cl = FlinkJobMain.class.getClassLoader();
            URL url = cl.getResource("mappings");
            if (url == null) {
                LOG.error("Mappings directory not found in resources");
                return "No mappings found (directory not found)";
            }
            
            if (url.getProtocol().equals("jar")) {
                String path = url.getPath();
                String jarPath = path.substring(5, path.indexOf("!"));
                try (JarFile jar = new JarFile(jarPath)) {
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        if (name.startsWith("mappings/") && name.endsWith("_mapping.json")) {
                            mappingTypes.add(name.substring("mappings/".length(), 
                                name.length() - "_mapping.json".length()));
                        }
                    }
                }
            } else {
                File mappingsDir = new File(url.toURI());
                if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                    for (File file : mappingsDir.listFiles()) {
                        if (file.getName().endsWith("_mapping.json")) {
                            mappingTypes.add(file.getName().substring(0, 
                                file.getName().length() - "_mapping.json".length()));
                        }
                    }
                }
            }
            
            LOG.info("Found {} mapping types: {}", mappingTypes.size(), mappingTypes);
            return String.join(", ", mappingTypes);
        } catch (Exception e) {
            LOG.error("Error listing mapping types: {}", e.getMessage());
            return "Error listing mapping types: " + e.getMessage();
        }
    }
}
