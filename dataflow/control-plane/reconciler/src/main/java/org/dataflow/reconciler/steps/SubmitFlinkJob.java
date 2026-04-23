package org.dataflow.reconciler.steps;

import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.infrastructure.flink.JobSubmissionService;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.Optional;

public class SubmitFlinkJob implements ReconcileStep {

    @Autowired
    private ApplicationContext context;

    @Override
    public String name() {
        return "SubmitFlinkJob";
    }

    @Override
    public void execute(StepContext ctx) {
        JobSubmissionService jss = context.getBean(JobSubmissionService.class);
        Optional<String> savepoint = ctx.read("savepoint.path");
        String jobId = jss.submit(ctx.tract(), savepoint);
        ctx.store("flink.jobId", jobId);
    }

    @Override
    public boolean verify(StepContext ctx) {
        return ctx.<String>read("flink.jobId")
                .flatMap(id -> ctx.flink().jobStatus(id))
                .map(s -> s.health() == ComponentHealth.RUNNING || s.health() == ComponentHealth.PENDING)
                .orElse(false);
    }
}
