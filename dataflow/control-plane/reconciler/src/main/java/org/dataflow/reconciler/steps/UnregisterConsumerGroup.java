package org.dataflow.reconciler.steps;

import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class UnregisterConsumerGroup implements ReconcileStep {

    @Override
    public String name() {
        return "UnregisterConsumerGroup";
    }

    @Override
    public void execute(StepContext ctx) {
        ctx.groups().deregister(RegisterConsumerGroup.consumerGroupFor(ctx));
    }

    @Override
    public boolean verify(StepContext ctx) {
        return !ctx.groups().exists(RegisterConsumerGroup.consumerGroupFor(ctx));
    }
}
