package org.dataflow.domain.port.dataplane;

import org.dataflow.domain.status.ComponentStatus;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface KafkaConnectClient {

    void registerConnector(String name, Map<String, String> config);

    void updateConnectorConfig(String name, Map<String, String> config);

    void pauseConnector(String name);

    void resumeConnector(String name);

    void unregisterConnector(String name);

    Optional<ComponentStatus> connectorStatus(String name);

    List<String> listConnectors();
}
