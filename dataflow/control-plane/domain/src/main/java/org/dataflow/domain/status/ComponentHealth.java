package org.dataflow.domain.status;

public enum ComponentHealth {
    UNKNOWN,
    PENDING,
    RUNNING,
    PAUSED,
    DEGRADED,
    FAILED,
    GONE
}
