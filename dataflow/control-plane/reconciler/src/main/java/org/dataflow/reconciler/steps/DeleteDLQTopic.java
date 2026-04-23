package org.dataflow.reconciler.steps;

import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class DeleteDLQTopic implements ReconcileStep {

    @Override
    public String name() {
        return "DeleteDLQTopic";
    }

    @Override
    public void execute(StepContext ctx) {
        String topic = CreateDLQTopic.dlqTopicFor(ctx);
        if (ctx.topics().topicExists(topic)) {
            ctx.topics().deleteTopic(topic);
        }
    }

    @Override
    public boolean verify(StepContext ctx) {
        return !ctx.topics().topicExists(CreateDLQTopic.dlqTopicFor(ctx));
    }
}
