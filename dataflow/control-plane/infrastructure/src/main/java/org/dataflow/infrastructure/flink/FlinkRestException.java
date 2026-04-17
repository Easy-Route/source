package org.dataflow.infrastructure.flink;

public class FlinkRestException extends RuntimeException {
    public FlinkRestException(String message, Throwable cause) {
        super(message, cause);
    }
}
