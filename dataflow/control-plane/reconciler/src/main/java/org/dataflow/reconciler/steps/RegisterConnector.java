package org.dataflow.reconciler.steps;

import org.dataflow.domain.source.Source;
import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.reconciler.plan.ReconcileStep;
import org.dataflow.reconciler.plan.StepContext;

public class RegisterConnector implements ReconcileStep {

    @Override
    public String name() {
        return "RegisterConnector";
    }

    @Override
    public void execute(StepContext ctx) {
        Source source = ctx.sources().findByName(ctx.tract().source()).orElseThrow();
        String connectorName = "dbz-" + source.name().value();
        var config = org.dataflow.infrastructure.connect.DebeziumConnectorConfigBuilder
                .buildPostgresConfig(source);
        ctx.connect().registerConnector(connectorName, config);
    }

    @Override
    public boolean verify(StepContext ctx) {
        String connectorName = "dbz-" + ctx.tract().source().value();
        return ctx.connect().connectorStatus(connectorName)
                .map(s -> s.health() == ComponentHealth.RUNNING || s.health() == ComponentHealth.PENDING)
                .orElse(false);
    }
}
