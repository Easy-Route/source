package org.dataflow.validator;

public record ValidationError(
        String path,
        String message,
        Severity severity,
        Integer line,
        Integer column
) {
    public ValidationError(String path, String message) {
        this(path, message, Severity.ERROR, null, null);
    }

    public static ValidationError at(String path, String message, int line, int column) {
        return new ValidationError(path, message, Severity.ERROR, line, column);
    }

    public static ValidationError warn(String path, String message) {
        return new ValidationError(path, message, Severity.WARNING, null, null);
    }

    public enum Severity {
        WARNING, ERROR
    }
}
