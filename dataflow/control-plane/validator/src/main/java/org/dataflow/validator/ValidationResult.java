package org.dataflow.validator;

import java.util.List;
import java.util.Optional;

public record ValidationResult(List<ValidationError> errors) {

    public ValidationResult {
        errors = errors == null ? List.of() : List.copyOf(errors);
    }

    public static ValidationResult ok() {
        return new ValidationResult(List.of());
    }

    public static ValidationResult of(List<ValidationError> errors) {
        return new ValidationResult(errors);
    }

    public boolean isValid() {
        return errors.stream().noneMatch(e -> e.severity() == ValidationError.Severity.ERROR);
    }

    public Optional<ValidationError> firstError() {
        return errors.stream()
                .filter(e -> e.severity() == ValidationError.Severity.ERROR)
                .findFirst();
    }

    public ValidationResult merge(ValidationResult other) {
        if (other.errors.isEmpty()) {
            return this;
        }
        if (errors.isEmpty()) {
            return other;
        }
        java.util.List<ValidationError> merged = new java.util.ArrayList<>(errors);
        merged.addAll(other.errors);
        return new ValidationResult(merged);
    }
}
