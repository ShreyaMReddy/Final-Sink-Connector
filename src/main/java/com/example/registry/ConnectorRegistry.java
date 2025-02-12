package com.example.registry;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple in-memory registry for managing multiple connectors.
 * This is a lightweight implementation that can be extended to use database storage in the future.
 */
public class ConnectorRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorRegistry.class);
    private static final Map<String, ConnectorInstance> registry = new ConcurrentHashMap<>();
    private final boolean enabled;

    public ConnectorRegistry(Config config) {
        this.enabled = config.getBoolean("registry.enabled");
        LOG.info("Connector registry initialized. Enabled: {}", enabled);
    }

    public void registerConnector(String connectorId, ConnectorInstance instance) {
        if (!enabled) return;
        
        registry.put(connectorId, instance);
        LOG.info("Registered connector: {}", connectorId);
    }

    public ConnectorInstance getConnector(String connectorId) {
        if (!enabled) return null;
        
        return registry.get(connectorId);
    }

    public void unregisterConnector(String connectorId) {
        if (!enabled) return;
        
        registry.remove(connectorId);
        LOG.info("Unregistered connector: {}", connectorId);
    }

    public static class ConnectorInstance {
        private final String connectorId;
        private final String connectorType;
        private final Config connectorConfig;

        public ConnectorInstance(String connectorId, String connectorType, Config connectorConfig) {
            this.connectorId = connectorId;
            this.connectorType = connectorType;
            this.connectorConfig = connectorConfig;
        }

        public String getConnectorId() { return connectorId; }
        public String getConnectorType() { return connectorType; }
        public Config getConnectorConfig() { return connectorConfig; }
    }
}
