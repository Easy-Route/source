package org.dataflow.flink.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TractConfig(
        Metadata metadata,
        Spec spec
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Metadata(String name, Map<String, String> labels) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Spec(
            Source source,
            Transformation transformation,
            List<Sink> sinks,
            Reliability reliability,
            Dlq dlq
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Source(String type, Connection connection, Publication publication) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Connection(String host, int port, String database, String user, String password) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Publication(String name, List<String> tables) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Transformation(Deduplication deduplication, Filter filter, Timezone timezone) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Deduplication(String key, Long ttlHours) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Filter(List<String> excludeOperations, String expression) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Timezone(String from, String to) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Sink(String name, String type, Map<String, String> connection, Mapping mapping) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Mapping(String mode, boolean primaryKeyModel, Map<String, String> tableOverrides) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Reliability(String deliveryGuarantee) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Dlq(boolean enabled, int retentionDays) {
    }
}
