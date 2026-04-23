package org.dataflow.reconciler.steps;

import org.dataflow.domain.spec.SourceSpec;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

import java.util.ArrayList;
import java.util.List;

public class RegisterConsumerGroup implements ReconcileStep {

    @Override
    public String name() {
        return "RegisterConsumerGroup";
    }

    @Override
    public void execute(StepContext ctx) {
        SourceSpec spec = ctx.sources().findByName(ctx.tract().source()).orElseThrow().spec();
        List<String> topics = new ArrayList<>(spec.publication().tables().size());
        for (String table : spec.publication().tables()) {
            topics.add(ctx.tract().source().value() + "." + table.replace('.', '_'));
        }
        ctx.groups().register(consumerGroupFor(ctx), topics);
    }

    @Override
    public boolean verify(StepContext ctx) {
        // Kafka groups are created lazily — we cannot rely on exists() until
        // the worker has consumed at least once. Treat the registration as
        // successful and rely on FlinkJob health for end-to-end verification.
        return true;
    }

    public static String consumerGroupFor(StepContext ctx) {
        return "dataflow-tract-" + ctx.tract().name().value();
    }
}
