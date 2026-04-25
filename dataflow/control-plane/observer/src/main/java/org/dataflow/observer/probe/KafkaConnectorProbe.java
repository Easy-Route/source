package org.dataflow.observer.probe;

import org.dataflow.domain.port.dataplane.KafkaConnectClient;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.status.ComponentStatus;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class KafkaConnectorProbe {

    private final KafkaConnectClient connect;

    public KafkaConnectorProbe(KafkaConnectClient connect) {
        this.connect = connect;
    }

    public Optional<ComponentStatus> probe(SourceName source) {
        return connect.connectorStatus("dbz-" + source.value());
    }
}
