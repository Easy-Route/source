package org.dataflow.reconciler.steps;

import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class VerifyComponentsRunning implements ReconcileStep {

    @Override
    public String name() {
        return "VerifyComponentsRunning";
    }

    @Override
    public void execute(StepContext ctx) {
        // pure verification — no side-effect; the verify() check below
        // is sufficient for the executor to record success or failure.
    }

    @Override
    public boolean verify(StepContext ctx) {
        boolean connectorOk = ctx.connect()
                .connectorStatus("dbz-" + ctx.tract().source().value())
                .map(s -> s.health() == ComponentHealth.RUNNING)
                .orElse(false);
        boolean jobOk = ctx.<String>read("flink.jobId")
                .or(() -> ctx.statuses().findByTract(ctx.tract().name()).flatMap(s -> s.flinkJobId()))
                .flatMap(id -> ctx.flink().jobStatus(id))
                .map(s -> s.health() == ComponentHealth.RUNNING)
                .orElse(false);
        return connectorOk && jobOk;
    }
}
