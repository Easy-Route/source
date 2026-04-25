package org.dataflow.observer.probe;

import org.dataflow.domain.port.dataplane.StarRocksClient;
import org.dataflow.domain.spec.SinkSpec;
import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.domain.status.ComponentKind;
import org.dataflow.domain.status.ComponentStatus;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Component
public class SinkConnectorProbe {

    private final StarRocksClient starRocks;

    public SinkConnectorProbe(StarRocksClient starRocks) {
        this.starRocks = starRocks;
    }

    public List<ComponentStatus> probe(List<SinkSpec> sinks) {
        return sinks.stream().map(this::probeOne).toList();
    }

    private ComponentStatus probeOne(SinkSpec sink) {
        String database = sink.connection().required("database");
        boolean reachable;
        try {
            reachable = starRocks.databaseExists(database);
        } catch (RuntimeException e) {
            reachable = false;
        }
        return new ComponentStatus(
                ComponentKind.SINK_CONNECTOR,
                sink.name(),
                reachable ? ComponentHealth.RUNNING : ComponentHealth.DEGRADED,
                Map.of("database", database, "type", sink.type()),
                Instant.now()
        );
    }
}
