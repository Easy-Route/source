package org.dataflow.domain.source;

import org.dataflow.domain.spec.SourceSpec;

import java.time.Instant;
import java.util.Objects;

public final class Source {
    private final SourceName name;
    private SourceSpec spec;
    private final Instant createdAt;
    private Instant updatedAt;

    private Source(SourceName name, SourceSpec spec, Instant createdAt, Instant updatedAt) {
        this.name = Objects.requireNonNull(name);
        this.spec = Objects.requireNonNull(spec);
        this.createdAt = Objects.requireNonNull(createdAt);
        this.updatedAt = Objects.requireNonNull(updatedAt);
    }

    public static Source register(SourceName name, SourceSpec spec, Instant now) {
        return new Source(name, spec, now, now);
    }

    public static Source restore(SourceName name, SourceSpec spec, Instant createdAt, Instant updatedAt) {
        return new Source(name, spec, createdAt, updatedAt);
    }

    public void replaceSpec(SourceSpec newSpec, Instant now) {
        this.spec = Objects.requireNonNull(newSpec);
        this.updatedAt = now;
    }

    public SourceName name() {
        return name;
    }

    public SourceSpec spec() {
        return spec;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }
}
