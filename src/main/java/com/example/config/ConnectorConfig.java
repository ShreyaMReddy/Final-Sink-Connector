package com.example.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class ConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorConfig.class);
    private final Config config;

    public ConnectorConfig(String configPath) {
        Config baseConfig = ConfigFactory.load("connector.conf");
        if (configPath != null && new File(configPath).exists()) {
            LOG.info("Loading configuration from: {}", configPath);
            this.config = ConfigFactory.parseFile(new File(configPath))
                    .withFallback(baseConfig)
                    .resolve();
        } else {
            LOG.info("Using default configuration");
            this.config = baseConfig;
        }
    }

    public Config getConfig() {
        return config;
    }

    // Bridge method to support existing Properties-based code
    public Properties toProperties() {
        Properties props = new Properties();
        // Copy existing properties
        props.setProperty("elasticsearch.hosts", config.getString("elasticsearch.hosts"));
        props.setProperty("elasticsearch.batch.size", String.valueOf(config.getInt("elasticsearch.batch.size")));
        props.setProperty("elasticsearch.flush.interval.ms", String.valueOf(config.getLong("elasticsearch.flush.interval.ms")));
        return props;
    }
}
