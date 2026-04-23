package org.dataflow.reconciler.steps;

import org.dataflow.infrastructure.flink.FlinkProperties;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public class SavepointFlinkJob implements ReconcileStep {

    @Autowired
    private ApplicationContext context;

    @Override
    public String name() {
        return "SavepointFlinkJob";
    }

    @Override
    public void execute(StepContext ctx) {
        FlinkProperties props = context.getBean(FlinkProperties.class);
        String jobId = ctx.statuses().findByTract(ctx.tract().name())
                .flatMap(status -> status.flinkJobId())
                .orElseThrow(() -> new IllegalStateException("No active Flink job for tract " + ctx.tract().name()));
        String savepointDir = props.savepointStorage() + "/" + ctx.tract().name().value();
        String requestId = ctx.flink().stopWithSavepoint(jobId, savepointDir);
        ctx.store("savepoint.requestId", requestId);
        ctx.store("savepoint.path", savepointDir);
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.read("savepoint.path").isPresent();
    }
}
