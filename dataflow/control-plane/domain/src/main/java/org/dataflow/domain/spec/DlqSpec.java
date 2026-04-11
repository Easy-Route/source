package org.dataflow.domain.spec;

public record DlqSpec(boolean enabled, int retentionDays) {
    public DlqSpec {
        if (retentionDays < 1 || retentionDays > 365) {
            throw new IllegalArgumentException("dlq.retentionDays must be in [1; 365]");
        }
    }

    public static DlqSpec defaultEnabled() {
        return new DlqSpec(true, 14);
    }
}
