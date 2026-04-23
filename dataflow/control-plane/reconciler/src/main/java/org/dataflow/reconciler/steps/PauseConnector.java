package org.dataflow.reconciler.steps;

import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class PauseConnector implements ReconcileStep {

    @Override
    public String name() {
        return "PauseConnector";
    }

    @Override
    public void execute(StepContext ctx) {
        ctx.connect().pauseConnector("dbz-" + ctx.tract().source().value());
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.connect().connectorStatus("dbz-" + ctx.tract().source().value())
                .map(s -> s.health() == ComponentHealth.PAUSED)
                .orElse(false);
    }
}
