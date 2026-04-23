package org.dataflow.reconciler.steps;

import org.dataflow.domain.port.dataplane.KafkaTopicAdmin;
import org.dataflow.domain.spec.SourceSpec;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class CreateSourceTopics implements ReconcileStep {

    @Override
    public String name() {
        return "CreateSourceTopics";
    }

    @Override
    public void execute(StepContext ctx) {
        SourceSpec spec = ctx.sources().findByName(ctx.tract().source())
                .orElseThrow(() -> new IllegalStateException(
                        "Source " + ctx.tract().source() + " not found"))
                .spec();
        for (String table : spec.publication().tables()) {
            String topic = ctx.tract().source().value() + "." + table.replace('.', '_');
            ctx.topics().createTopic(new KafkaTopicAdmin.TopicSpec(
                    topic, 6, (short) 1,
                    7L * 24 * 60 * 60 * 1000,
                    "delete"));
        }
    }

    @Override
    public boolean verify(StepContext ctx) {
        SourceSpec spec = ctx.sources().findByName(ctx.tract().source()).orElseThrow().spec();
        for (String table : spec.publication().tables()) {
            String topic = ctx.tract().source().value() + "." + table.replace('.', '_');
            if (!ctx.topics().topicExists(topic)) {
                return false;
            }
        }
        return true;
    }
}
