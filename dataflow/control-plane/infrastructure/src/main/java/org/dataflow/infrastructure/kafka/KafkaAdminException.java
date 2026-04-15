package org.dataflow.infrastructure.kafka;

public class KafkaAdminException extends RuntimeException {
    public KafkaAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
