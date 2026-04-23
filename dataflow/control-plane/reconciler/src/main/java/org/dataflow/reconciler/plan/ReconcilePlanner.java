package org.dataflow.reconciler.plan;

import org.dataflow.domain.tract.DesiredState;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractStatus;
import org.dataflow.reconciler.steps.AddSink;
import org.dataflow.reconciler.steps.CancelFlinkJob;
import org.dataflow.reconciler.steps.CreateDLQTopic;
import org.dataflow.reconciler.steps.DeleteDLQTopic;
import org.dataflow.reconciler.steps.PauseConnector;
import org.dataflow.reconciler.steps.RegisterConsumerGroup;
import org.dataflow.reconciler.steps.RemoveSink;
import org.dataflow.reconciler.steps.ResumeConnector;
import org.dataflow.reconciler.steps.SavepointFlinkJob;
import org.dataflow.reconciler.steps.SubmitFlinkJob;
import org.dataflow.reconciler.steps.UnregisterConsumerGroup;
import org.dataflow.reconciler.steps.VerifyComponentsRunning;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ReconcilePlanner {

    public ReconcilePlan plan(Tract tract, TractStatus observed) {
        DesiredState desired = tract.desiredState();
        return switch (desired) {
            case DRAFTED -> ReconcilePlan.noop();
            case DEPLOYED -> planDeploy(tract, observed);
            case SUSPENDED -> planSuspend(tract, observed);
            case DELETED -> planDelete(tract, observed);
            case FAILED -> ReconcilePlan.noop();
        };
    }

    private ReconcilePlan planDeploy(Tract tract, TractStatus observed) {
        List<ReconcileStep> steps = new ArrayList<>();
        if (observed.flinkJobId().isEmpty()) {
            steps.add(new RegisterConsumerGroup());
            steps.add(new CreateDLQTopic());
            steps.add(new SubmitFlinkJob());
            steps.add(new VerifyComponentsRunning());
            return new ReconcilePlan(steps, "deploy: cold start");
        }
        if (tract.specVersion().value() != observed.observedSpecVersion()) {
            steps.add(new SavepointFlinkJob());
            steps.add(new SubmitFlinkJob());
            steps.add(new VerifyComponentsRunning());
            return new ReconcilePlan(steps, "deploy: spec version drift");
        }
        if (observed.connector().map(c -> c.health().name().equals("PAUSED")).orElse(false)) {
            steps.add(new ResumeConnector());
            steps.add(new VerifyComponentsRunning());
            return new ReconcilePlan(steps, "deploy: resume from paused");
        }
        return ReconcilePlan.noop();
    }

    private ReconcilePlan planSuspend(Tract tract, TractStatus observed) {
        List<ReconcileStep> steps = new ArrayList<>();
        observed.flinkJobId().ifPresent(id -> steps.add(new SavepointFlinkJob()));
        if (observed.connector().isPresent()) {
            steps.add(new PauseConnector());
        }
        return new ReconcilePlan(steps, "suspend: savepoint and pause");
    }

    private ReconcilePlan planDelete(Tract tract, TractStatus observed) {
        List<ReconcileStep> steps = new ArrayList<>();
        observed.flinkJobId().ifPresent(id -> steps.add(new CancelFlinkJob()));
        steps.add(new UnregisterConsumerGroup());
        steps.add(new DeleteDLQTopic());
        return new ReconcilePlan(steps, "delete: release tract-owned runtime");
    }

    public ReconcilePlan planSinkAddition(Tract tract, String sinkName) {
        return new ReconcilePlan(List.of(
                new SavepointFlinkJob(),
                new AddSink(sinkName),
                new VerifyComponentsRunning()
        ), "hot sink add: savepoint -> add side-output -> restart");
    }

    public ReconcilePlan planSinkRemoval(Tract tract, String sinkName) {
        return new ReconcilePlan(List.of(
                new SavepointFlinkJob(),
                new RemoveSink(sinkName),
                new VerifyComponentsRunning()
        ), "hot sink remove: savepoint -> remove side-output -> restart");
    }
}
