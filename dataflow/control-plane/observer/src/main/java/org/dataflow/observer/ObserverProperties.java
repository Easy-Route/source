package org.dataflow.observer;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "dataflow.observer")
public record ObserverProperties(
        Duration topicProbeInterval,
        Duration connectorProbeInterval,
        Duration flinkProbeInterval,
        Duration sinkProbeInterval
) {
    public ObserverProperties {
        if (topicProbeInterval == null || topicProbeInterval.isZero()) {
            topicProbeInterval = Duration.ofSeconds(30);
        }
        if (connectorProbeInterval == null || connectorProbeInterval.isZero()) {
            connectorProbeInterval = Duration.ofSeconds(15);
        }
        if (flinkProbeInterval == null || flinkProbeInterval.isZero()) {
            flinkProbeInterval = Duration.ofSeconds(10);
        }
        if (sinkProbeInterval == null || sinkProbeInterval.isZero()) {
            sinkProbeInterval = Duration.ofSeconds(30);
        }
    }
}
