package org.dataflow.domain.spec;

public record LineageSpec(boolean preserveSourceMetadata) {
    public static LineageSpec defaults() {
        return new LineageSpec(true);
    }
}
