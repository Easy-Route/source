package org.dataflow.domain.spec;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public record TractSpec(
        String apiVersion,
        String kind,
        SpecMetadata metadata,
        SourceSpec source,
        Optional<TransformationSpec> transformation,
        List<SinkSpec> sinks,
        Optional<ReliabilitySpec> reliability,
        Optional<DlqSpec> dlq,
        Optional<ObservabilitySpec> observability,
        Optional<LineageSpec> lineage,
        String rawDocument
) {
    public TractSpec {
        if (apiVersion == null || apiVersion.isBlank()) {
            throw new IllegalArgumentException("apiVersion is required");
        }
        if (kind == null || !kind.equals("Tract")) {
            throw new IllegalArgumentException("kind must be 'Tract'");
        }
        if (metadata == null) {
            throw new IllegalArgumentException("metadata is required");
        }
        if (source == null) {
            throw new IllegalArgumentException("source is required");
        }
        if (sinks == null || sinks.isEmpty()) {
            throw new IllegalArgumentException("at least one sink is required");
        }
        sinks = List.copyOf(sinks);
        transformation = transformation == null ? Optional.empty() : transformation;
        reliability = reliability == null ? Optional.empty() : reliability;
        dlq = dlq == null ? Optional.empty() : dlq;
        observability = observability == null ? Optional.empty() : observability;
        lineage = lineage == null ? Optional.empty() : lineage;
    }

    public String name() {
        return metadata.name();
    }

    public record SpecMetadata(String name, Map<String, String> labels) {
        public SpecMetadata {
            labels = labels == null ? Map.of() : Map.copyOf(labels);
        }
    }
}
