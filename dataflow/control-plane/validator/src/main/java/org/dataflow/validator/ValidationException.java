package org.dataflow.validator;

public class ValidationException extends RuntimeException {

    private final ValidationResult result;

    public ValidationException(ValidationResult result) {
        super(result.firstError().map(e -> e.path() + ": " + e.message()).orElse("Validation failed"));
        this.result = result;
    }

    public ValidationResult result() {
        return result;
    }
}
