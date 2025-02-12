package com.example.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.util.Properties;

public class ConfigurationManager {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationManager.class);
    private final Config config;
    private final Properties properties;
    private static ConfigurationManager instance;

    private ConfigurationManager() {
        try {
            // Load application.properties
            properties = PropertiesLoaderUtils.loadAllProperties("application.properties");
            
            // Load TypeSafe Config (will automatically merge application.conf if present)
            Config baseConfig = ConfigFactory.load();
            
            // Check for external config file
            String externalConfig = System.getProperty("config.file");
            if (externalConfig != null && new File(externalConfig).exists()) {
                config = ConfigFactory.parseFile(new File(externalConfig))
                        .withFallback(baseConfig)
                        .resolve();
                LOG.info("Loaded external configuration from: {}", externalConfig);
            } else {
                config = baseConfig.resolve();
                LOG.info("Using default configuration");
            }
        } catch (Exception e) {
            LOG.error("Failed to load configuration", e);
            throw new RuntimeException("Configuration initialization failed", e);
        }
    }

    public static synchronized ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }

    public Config getConfig() {
        return config;
    }

    public Properties getProperties() {
        return properties;
    }

    // Elasticsearch Configuration
    public String getElasticsearchHosts() {
        return properties.getProperty("elasticsearch.hosts", "http://elasticsearch:9200");
    }

    public int getElasticsearchBulkSize() {
        return Integer.parseInt(properties.getProperty("elasticsearch.bulk.size", "5"));
    }

    public long getElasticsearchBatchTimeout() {
        return Long.parseLong(properties.getProperty("elasticsearch.batch.timeout.ms", "3000"));
    }

    // Window Configuration
    public boolean isWindowingEnabled() {
        return Boolean.parseBoolean(properties.getProperty("window.enabled", "false"));
    }

    public int getWindowSizeMinutes() {
        return Integer.parseInt(properties.getProperty("window.size.minutes", "5"));
    }

    // Registry Configuration
    public boolean isRegistryEnabled() {
        return Boolean.parseBoolean(properties.getProperty("registry.enabled", "false"));
    }

    public String getRegistryStorageType() {
        return properties.getProperty("registry.storage.type", "memory");
    }

    // Security Configuration
    public boolean isEncryptionEnabled() {
        return Boolean.parseBoolean(properties.getProperty("security.encryption.enabled", "false"));
    }

    public String getEncryptionAlgorithm() {
        return properties.getProperty("security.encryption.algorithm", "AES/GCM/NoPadding");
    }

    // Monitoring Configuration
    public boolean isMonitoringEnabled() {
        return Boolean.parseBoolean(properties.getProperty("monitoring.enabled", "true"));
    }

    public long getMetricsLogInterval() {
        return Long.parseLong(properties.getProperty("monitoring.metrics.log.interval.ms", "60000"));
    }
}
