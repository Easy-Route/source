package org.dataflow.domain.spec;

public record ObservabilitySpec(boolean metricsEnabled) {
    public static ObservabilitySpec enabled() {
        return new ObservabilitySpec(true);
    }
}
