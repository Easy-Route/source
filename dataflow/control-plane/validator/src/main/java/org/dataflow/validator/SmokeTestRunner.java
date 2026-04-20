package org.dataflow.validator;

import org.dataflow.domain.port.dataplane.FlinkJobClient;
import org.dataflow.domain.port.dataplane.KafkaConnectClient;
import org.dataflow.domain.port.dataplane.StarRocksClient;
import org.dataflow.domain.spec.SinkSpec;
import org.dataflow.domain.spec.TractSpec;
import org.dataflow.domain.status.ComponentHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SmokeTestRunner {

    private static final Logger log = LoggerFactory.getLogger(SmokeTestRunner.class);

    private final KafkaConnectClient connect;
    private final FlinkJobClient flink;
    private final StarRocksClient starRocks;

    public SmokeTestRunner(KafkaConnectClient connect, FlinkJobClient flink, StarRocksClient starRocks) {
        this.connect = connect;
        this.flink = flink;
        this.starRocks = starRocks;
    }

    public ValidationResult runAfterDeploy(TractSpec spec, String connectorName, String flinkJobId) {
        boolean connectorOk = connect.connectorStatus(connectorName)
                .map(s -> s.health() == ComponentHealth.RUNNING)
                .orElse(false);
        if (!connectorOk) {
            return ValidationResult.of(List.of(new ValidationError(
                    "components.connector",
                    "Connector " + connectorName + " is not RUNNING after deploy")));
        }

        boolean jobOk = flink.jobStatus(flinkJobId)
                .map(s -> s.health() == ComponentHealth.RUNNING)
                .orElse(false);
        if (!jobOk) {
            return ValidationResult.of(List.of(new ValidationError(
                    "components.flinkJob",
                    "Flink job " + flinkJobId + " is not RUNNING after deploy")));
        }

        for (SinkSpec sink : spec.sinks()) {
            String database = sink.connection().required("database");
            try {
                starRocks.writeHeartbeat(database, spec.name());
            } catch (Exception e) {
                log.warn("Heartbeat write failed for {}/{}: {}", spec.name(), sink.name(), e.toString());
                return ValidationResult.of(List.of(new ValidationError(
                        "components.sinks." + sink.name(),
                        "Heartbeat write to " + database + " failed: " + e.getMessage())));
            }
        }

        List<FlinkJobClient.JobMetric> metrics = flink.jobMetrics(flinkJobId, List.of("numRecordsIn"));
        long received = metrics.stream()
                .filter(m -> m.id().endsWith("numRecordsIn"))
                .mapToLong(m -> {
                    try {
                        return Long.parseLong(m.value());
                    } catch (NumberFormatException ex) {
                        return 0L;
                    }
                })
                .sum();
        if (received < 1) {
            return ValidationResult.of(List.of(ValidationError.warn(
                    "components.flinkJob.numRecordsIn",
                    "Flink job has not yet received events; smoke-test passed warning state")));
        }
        return ValidationResult.ok();
    }
}
