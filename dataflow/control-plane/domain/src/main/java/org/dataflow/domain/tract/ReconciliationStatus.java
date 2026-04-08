package org.dataflow.domain.tract;

public enum ReconciliationStatus {
    RECONCILED,
    RECONCILING,
    DRIFT,
    STUCK;

    public boolean isTerminalForCycle() {
        return this == RECONCILED || this == STUCK;
    }
}
