package org.dataflow.reconciler;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public final class RetryPolicy {

    private final ReconcilerProperties props;

    public RetryPolicy(ReconcilerProperties props) {
        this.props = props;
    }

    public Duration backoff(int attempt) {
        long base = (long) (props.retryInitial().toMillis() * Math.pow(props.retryMultiplier(), attempt - 1));
        long capped = Math.min(base, props.retryMax().toMillis());
        double jitter = (ThreadLocalRandom.current().nextDouble() * 2 - 1) * props.retryJitter();
        long withJitter = (long) (capped * (1 + jitter));
        return Duration.ofMillis(Math.max(0, withJitter));
    }

    public int maxAttempts() {
        return props.maxAttempts();
    }
}
