package org.dataflow.domain.tract;

import org.dataflow.domain.status.ComponentStatus;
import org.dataflow.domain.status.TopicStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record TractStatus(
        TractName tract,
        DesiredState desiredState,
        ReconciliationStatus reconciliationStatus,
        Optional<ComponentStatus> connector,
        List<TopicStatus> topics,
        Optional<ComponentStatus> flinkJob,
        List<ComponentStatus> sinks,
        Optional<String> flinkJobId,
        Optional<String> lastError,
        long observedSpecVersion,
        Instant observedAt
) {
    public TractStatus {
        topics = topics == null ? List.of() : List.copyOf(topics);
        sinks = sinks == null ? List.of() : List.copyOf(sinks);
        connector = connector == null ? Optional.empty() : connector;
        flinkJob = flinkJob == null ? Optional.empty() : flinkJob;
        flinkJobId = flinkJobId == null ? Optional.empty() : flinkJobId;
        lastError = lastError == null ? Optional.empty() : lastError;
    }

    public boolean inSync() {
        return reconciliationStatus == ReconciliationStatus.RECONCILED;
    }
}
