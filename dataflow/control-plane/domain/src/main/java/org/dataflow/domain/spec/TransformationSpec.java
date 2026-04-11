package org.dataflow.domain.spec;

import java.util.List;
import java.util.Optional;

public record TransformationSpec(
        Optional<DeduplicationSpec> deduplication,
        Optional<FilterSpec> filter,
        Optional<TimezoneSpec> timezone
) {
    public TransformationSpec {
        deduplication = deduplication == null ? Optional.empty() : deduplication;
        filter = filter == null ? Optional.empty() : filter;
        timezone = timezone == null ? Optional.empty() : timezone;
    }

    public record DeduplicationSpec(String key, long ttlHours) {
        public DeduplicationSpec {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("deduplication.key is required");
            }
            if (ttlHours <= 0) {
                ttlHours = 24;
            }
        }
    }

    public record FilterSpec(List<Operation> excludeOperations, Optional<String> expression) {
        public FilterSpec {
            excludeOperations = excludeOperations == null ? List.of() : List.copyOf(excludeOperations);
            expression = expression == null ? Optional.empty() : expression;
        }
    }

    public enum Operation {
        INSERT, UPDATE, DELETE, TRUNCATE, DDL
    }

    public record TimezoneSpec(String from, String to) {
        public TimezoneSpec {
            if (from == null || from.isBlank()) {
                throw new IllegalArgumentException("timezone.from is required");
            }
            if (to == null || to.isBlank()) {
                throw new IllegalArgumentException("timezone.to is required");
            }
        }
    }
}
