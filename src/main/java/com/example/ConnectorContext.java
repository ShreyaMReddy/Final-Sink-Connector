package com.example;

import java.io.Serializable;

public class ConnectorContext implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String connectorType;
    private final String connectorId;
    private final String sourceTopic;

    public ConnectorContext(String connectorType, String connectorId, String sourceTopic) {
        this.connectorType = connectorType;
        this.connectorId = connectorId;
        this.sourceTopic = sourceTopic;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }
}
