package org.dataflow.infrastructure.connect;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataflow.connect")
public record KafkaConnectProperties(
        String url,
        int connectTimeoutMs,
        int readTimeoutMs
) {
    public KafkaConnectProperties {
        if (url == null || url.isBlank()) {
            url = "http://kafka-connect:8083";
        }
        if (connectTimeoutMs <= 0) {
            connectTimeoutMs = 5_000;
        }
        if (readTimeoutMs <= 0) {
            readTimeoutMs = 30_000;
        }
    }
}
