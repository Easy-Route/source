package org.dataflow.reconciler.steps;

import org.dataflow.domain.spec.SourceSpec;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class DeleteSourceTopics implements ReconcileStep {

    @Override
    public String name() {
        return "DeleteSourceTopics";
    }

    @Override
    public void execute(StepContext ctx) {
        SourceSpec spec = ctx.sources().findByName(ctx.tract().source()).orElseThrow().spec();
        for (String table : spec.publication().tables()) {
            String topic = ctx.tract().source().value() + "." + table.replace('.', '_');
            ctx.topics().deleteTopic(topic);
        }
    }

    @Override
    public boolean verify(StepContext ctx) {
        SourceSpec spec = ctx.sources().findByName(ctx.tract().source()).orElseThrow().spec();
        for (String table : spec.publication().tables()) {
            String topic = ctx.tract().source().value() + "." + table.replace('.', '_');
            if (ctx.topics().topicExists(topic)) {
                return false;
            }
        }
        return true;
    }
}
