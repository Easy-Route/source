package org.dataflow.domain.tract;

import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.spec.TractSpec;

import java.time.Instant;
import java.util.Objects;

public final class Tract {
    private final TractName name;
    private final SourceName source;
    private DesiredState desiredState;
    private TractSpec spec;
    private TractSpecVersion specVersion;
    private final Instant createdAt;
    private Instant updatedAt;

    private Tract(TractName name,
                  SourceName source,
                  DesiredState desiredState,
                  TractSpec spec,
                  TractSpecVersion specVersion,
                  Instant createdAt,
                  Instant updatedAt) {
        this.name = Objects.requireNonNull(name);
        this.source = Objects.requireNonNull(source);
        this.desiredState = Objects.requireNonNull(desiredState);
        this.spec = Objects.requireNonNull(spec);
        this.specVersion = Objects.requireNonNull(specVersion);
        this.createdAt = Objects.requireNonNull(createdAt);
        this.updatedAt = Objects.requireNonNull(updatedAt);
    }

    public static Tract draft(TractName name, SourceName source, TractSpec spec, Instant now) {
        return new Tract(name, source, DesiredState.DRAFTED, spec, new TractSpecVersion(1), now, now);
    }

    public static Tract restore(TractName name,
                                SourceName source,
                                DesiredState state,
                                TractSpec spec,
                                TractSpecVersion specVersion,
                                Instant createdAt,
                                Instant updatedAt) {
        return new Tract(name, source, state, spec, specVersion, createdAt, updatedAt);
    }

    public void deploy(Instant now) {
        transitionTo(DesiredState.DEPLOYED, now);
    }

    public void suspend(Instant now) {
        transitionTo(DesiredState.SUSPENDED, now);
    }

    public void resume(Instant now) {
        transitionTo(DesiredState.DEPLOYED, now);
    }

    public void markFailed(Instant now) {
        transitionTo(DesiredState.FAILED, now);
    }

    public void delete(Instant now) {
        transitionTo(DesiredState.DELETED, now);
    }

    public void replaceSpec(TractSpec newSpec, Instant now) {
        if (desiredState == DesiredState.DELETED) {
            throw new IllegalStateException("Cannot update a deleted tract");
        }
        this.spec = newSpec;
        this.specVersion = specVersion.next();
        this.updatedAt = now;
    }

    private void transitionTo(DesiredState target, Instant now) {
        if (!desiredState.canTransitionTo(target)) {
            throw new InvalidTransitionException(desiredState, target);
        }
        this.desiredState = target;
        this.updatedAt = now;
    }

    public TractName name() {
        return name;
    }

    public SourceName source() {
        return source;
    }

    public DesiredState desiredState() {
        return desiredState;
    }

    public TractSpec spec() {
        return spec;
    }

    public TractSpecVersion specVersion() {
        return specVersion;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }
}
