package org.dataflow.api.dto;

import org.dataflow.domain.tract.TractStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record TractStatusDto(
        String tract,
        String desiredState,
        String reconciliationStatus,
        Map<String, Object> components,
        long observedSpecVersion,
        String lastError,
        Instant observedAt
) {
    public static TractStatusDto from(TractStatus status) {
        Map<String, Object> components = Map.of(
                "connector", status.connector().map(c -> Map.of(
                        "kind", c.kind().name(),
                        "id", c.identifier(),
                        "health", c.health().name()
                )).orElse(Map.of()),
                "topics", status.topics().stream().map(t -> Map.of(
                        "name", t.topicName(),
                        "partitions", t.partitions(),
                        "replicationFactor", t.replicationFactor(),
                        "health", t.health().name()
                )).toList(),
                "flinkJob", status.flinkJob().map(c -> Map.of(
                        "id", c.identifier(),
                        "health", c.health().name()
                )).orElse(Map.of()),
                "sinks", status.sinks().stream().map(s -> Map.of(
                        "name", s.identifier(),
                        "health", s.health().name()
                )).toList()
        );
        return new TractStatusDto(
                status.tract().value(),
                status.desiredState().name(),
                status.reconciliationStatus().name(),
                components,
                status.observedSpecVersion(),
                status.lastError().orElse(null),
                status.observedAt()
        );
    }

    public static TractStatusDto unknown(String tract) {
        return new TractStatusDto(tract, "UNKNOWN", "UNKNOWN",
                Map.of(), 0L, "no observed state yet", Instant.now());
    }

    public static List<Map<String, Object>> noEvents() {
        return List.of();
    }
}
