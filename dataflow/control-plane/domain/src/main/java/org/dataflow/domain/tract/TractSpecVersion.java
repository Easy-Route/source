package org.dataflow.domain.tract;

public record TractSpecVersion(long value) {
    public TractSpecVersion {
        if (value < 1) {
            throw new IllegalArgumentException("Spec version must be >= 1");
        }
    }

    public TractSpecVersion next() {
        return new TractSpecVersion(value + 1);
    }
}
