package org.dataflow.reconciler.plan;

import org.dataflow.domain.port.repository.ReconciliationLog;
import org.dataflow.domain.tract.TractName;
import org.dataflow.reconciler.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class PlanExecutor {

    private static final Logger log = LoggerFactory.getLogger(PlanExecutor.class);

    private final ReconciliationLog reconciliationLog;

    public PlanExecutor(ReconciliationLog reconciliationLog) {
        this.reconciliationLog = reconciliationLog;
    }

    public Result execute(ReconcilePlan plan, StepContext ctx, RetryPolicy retry) {
        TractName tractName = ctx.tract().name();
        for (ReconcileStep step : plan.steps()) {
            Result result = runWithRetry(step, ctx, retry, tractName);
            if (result != Result.SUCCEEDED) {
                return result;
            }
        }
        return Result.SUCCEEDED;
    }

    private Result runWithRetry(ReconcileStep step, StepContext ctx, RetryPolicy retry, TractName tract) {
        for (int attempt = 1; attempt <= retry.maxAttempts(); attempt++) {
            long started = System.nanoTime();
            try {
                step.execute(ctx);
                if (!step.verify(ctx)) {
                    throw new IllegalStateException("verify() returned false for " + step.name());
                }
                long durationMs = (System.nanoTime() - started) / 1_000_000L;
                reconciliationLog.append(tract, step.name(),
                        ReconciliationLog.Outcome.SUCCEEDED,
                        "attempt=" + attempt, durationMs);
                return Result.SUCCEEDED;
            } catch (RuntimeException e) {
                long durationMs = (System.nanoTime() - started) / 1_000_000L;
                String detail = "attempt=" + attempt + " error=" + e.getMessage();
                reconciliationLog.append(tract, step.name(),
                        ReconciliationLog.Outcome.FAILED, detail, durationMs);
                log.warn("Step {} failed (attempt {}/{}): {}",
                        step.name(), attempt, retry.maxAttempts(), e.toString());
                if (attempt == retry.maxAttempts()) {
                    return Result.STUCK;
                }
                sleep(retry.backoff(attempt));
            }
        }
        return Result.STUCK;
    }

    private void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public enum Result {
        SUCCEEDED, STUCK, ABORTED
    }
}
