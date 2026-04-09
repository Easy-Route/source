package org.dataflow.domain.status;

import java.time.Instant;

public record TopicStatus(
        String topicName,
        int partitions,
        short replicationFactor,
        long retentionMs,
        ComponentHealth health,
        Instant observedAt
) {
}
