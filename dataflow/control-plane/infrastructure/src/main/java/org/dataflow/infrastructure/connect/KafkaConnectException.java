package org.dataflow.infrastructure.connect;

public class KafkaConnectException extends RuntimeException {
    public KafkaConnectException(String message, Throwable cause) {
        super(message, cause);
    }
}
