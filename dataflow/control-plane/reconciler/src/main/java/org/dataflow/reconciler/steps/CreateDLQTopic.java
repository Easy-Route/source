package org.dataflow.reconciler.steps;

import org.dataflow.domain.port.dataplane.KafkaTopicAdmin;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class CreateDLQTopic implements ReconcileStep {

    @Override
    public String name() {
        return "CreateDLQTopic";
    }

    @Override
    public void execute(StepContext ctx) {
        int retentionDays = ctx.tract().spec().dlq().map(d -> d.retentionDays()).orElse(14);
        long retentionMs = retentionDays * 24L * 60 * 60 * 1000;
        ctx.topics().createTopic(new KafkaTopicAdmin.TopicSpec(
                dlqTopicFor(ctx),
                3, (short) 1, retentionMs, "delete"));
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.topics().topicExists(dlqTopicFor(ctx));
    }

    public static String dlqTopicFor(StepContext ctx) {
        return "dlq." + ctx.tract().name().value();
    }
}
