package org.dataflow.reconciler.steps;

import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class UnregisterConnector implements ReconcileStep {

    @Override
    public String name() {
        return "UnregisterConnector";
    }

    @Override
    public void execute(StepContext ctx) {
        ctx.connect().unregisterConnector("dbz-" + ctx.tract().source().value());
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.connect().connectorStatus("dbz-" + ctx.tract().source().value()).isEmpty();
    }
}
