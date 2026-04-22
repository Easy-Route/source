package org.dataflow.reconciler;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "dataflow.reconciler")
public record ReconcilerProperties(
        Duration period,
        Duration stuckAfter,
        int maxAttempts,
        Duration retryInitial,
        Duration retryMax,
        double retryMultiplier,
        double retryJitter
) {
    public ReconcilerProperties {
        if (period == null || period.isZero() || period.isNegative()) {
            period = Duration.ofSeconds(10);
        }
        if (stuckAfter == null || stuckAfter.isZero() || stuckAfter.isNegative()) {
            stuckAfter = Duration.ofMinutes(10);
        }
        if (maxAttempts <= 0) {
            maxAttempts = 5;
        }
        if (retryInitial == null || retryInitial.isZero() || retryInitial.isNegative()) {
            retryInitial = Duration.ofSeconds(2);
        }
        if (retryMax == null || retryMax.isZero() || retryMax.isNegative()) {
            retryMax = Duration.ofSeconds(60);
        }
        if (retryMultiplier <= 1.0) {
            retryMultiplier = 2.0;
        }
        if (retryJitter < 0 || retryJitter > 1.0) {
            retryJitter = 0.2;
        }
    }
}
