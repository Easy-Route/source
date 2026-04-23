package org.dataflow.reconciler.steps;

import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class ResumeConnector implements ReconcileStep {

    @Override
    public String name() {
        return "ResumeConnector";
    }

    @Override
    public void execute(StepContext ctx) {
        ctx.connect().resumeConnector("dbz-" + ctx.tract().source().value());
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.connect().connectorStatus("dbz-" + ctx.tract().source().value())
                .map(s -> s.health() == ComponentHealth.RUNNING)
                .orElse(false);
    }
}
