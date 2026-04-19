package org.dataflow.validator;

public class SpecParseException extends RuntimeException {
    private final Integer line;
    private final Integer column;

    public SpecParseException(String message, Integer line, Integer column, Throwable cause) {
        super(message, cause);
        this.line = line;
        this.column = column;
    }

    public Integer line() {
        return line;
    }

    public Integer column() {
        return column;
    }
}
