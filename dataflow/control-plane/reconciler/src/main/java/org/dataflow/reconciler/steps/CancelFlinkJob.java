package org.dataflow.reconciler.steps;

import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class CancelFlinkJob implements ReconcileStep {

    @Override
    public String name() {
        return "CancelFlinkJob";
    }

    @Override
    public void execute(StepContext ctx) {
        ctx.statuses().findByTract(ctx.tract().name())
                .flatMap(status -> status.flinkJobId())
                .ifPresent(jobId -> ctx.flink().cancelJob(jobId));
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.statuses().findByTract(ctx.tract().name())
                .flatMap(status -> status.flinkJobId())
                .flatMap(jobId -> ctx.flink().jobStatus(jobId))
                .map(s -> !s.isHealthy())
                .orElse(true);
    }
}
