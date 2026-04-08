package org.dataflow.domain.tract;

public enum ChangeImpact {
    HOT,
    REQUIRES_RESTART,
    REQUIRES_RECREATE;

    public ChangeImpact merge(ChangeImpact other) {
        if (this == REQUIRES_RECREATE || other == REQUIRES_RECREATE) {
            return REQUIRES_RECREATE;
        }
        if (this == REQUIRES_RESTART || other == REQUIRES_RESTART) {
            return REQUIRES_RESTART;
        }
        return HOT;
    }
}
