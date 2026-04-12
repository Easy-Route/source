package org.dataflow.domain.source;

import org.dataflow.domain.status.ComponentStatus;
import org.dataflow.domain.status.TopicStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public record SourceStatus(
        SourceName source,
        Optional<ComponentStatus> debeziumConnector,
        List<TopicStatus> topics,
        Instant observedAt
) {
    public SourceStatus {
        topics = topics == null ? List.of() : List.copyOf(topics);
        debeziumConnector = debeziumConnector == null ? Optional.empty() : debeziumConnector;
    }
}
