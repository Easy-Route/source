package org.dataflow.domain.status;

import java.time.Instant;
import java.util.Map;

public record ComponentStatus(
        ComponentKind kind,
        String identifier,
        ComponentHealth health,
        Map<String, String> attributes,
        Instant observedAt
) {
    public ComponentStatus {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public boolean isHealthy() {
        return health == ComponentHealth.RUNNING;
    }
}
