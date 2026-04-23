package org.dataflow.reconciler.steps;

import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class AddSink implements ReconcileStep {

    private final String sinkName;

    public AddSink(String sinkName) {
        this.sinkName = sinkName;
    }

    @Override
    public String name() {
        return "AddSink(" + sinkName + ")";
    }

    @Override
    public void execute(StepContext ctx) {
        // Adding a side-output requires Flink topology change. The plan
        // pairs this step with SavepointFlinkJob and SubmitFlinkJob; the
        // resubmitted topology is built from the new spec which already
        // includes the added sink. This step is therefore a marker that
        // is expected to come between SavepointFlinkJob and SubmitFlinkJob.
        ctx.store("sink.added", sinkName);
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.tract().spec().sinks().stream().anyMatch(s -> s.name().equals(sinkName));
    }
}
