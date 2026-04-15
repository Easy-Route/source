package org.dataflow.infrastructure.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataflow.kafka")
public record KafkaProperties(
        String bootstrapServers,
        String securityProtocol,
        String saslMechanism,
        String saslJaasConfig,
        int requestTimeoutMs,
        int defaultReplicationFactor,
        int defaultPartitions,
        long defaultRetentionMs
) {
    public KafkaProperties {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            bootstrapServers = "localhost:9092";
        }
        if (securityProtocol == null) {
            securityProtocol = "PLAINTEXT";
        }
        if (requestTimeoutMs <= 0) {
            requestTimeoutMs = 30_000;
        }
        if (defaultReplicationFactor <= 0) {
            defaultReplicationFactor = 1;
        }
        if (defaultPartitions <= 0) {
            defaultPartitions = 6;
        }
        if (defaultRetentionMs <= 0) {
            defaultRetentionMs = 7L * 24 * 60 * 60 * 1000;
        }
    }
}
