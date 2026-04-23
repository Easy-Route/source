package org.dataflow.reconciler.steps;

import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class RemoveSink implements ReconcileStep {

    private final String sinkName;

    public RemoveSink(String sinkName) {
        this.sinkName = sinkName;
    }

    @Override
    public String name() {
        return "RemoveSink(" + sinkName + ")";
    }

    @Override
    public void execute(StepContext ctx) {
        ctx.store("sink.removed", sinkName);
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.tract().spec().sinks().stream().noneMatch(s -> s.name().equals(sinkName));
    }
}
