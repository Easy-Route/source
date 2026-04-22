package org.dataflow.reconciler;

import org.dataflow.domain.port.repository.ReconciliationLog;
import org.dataflow.domain.port.repository.TractStatusRepository;
import org.dataflow.domain.tract.ReconciliationStatus;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Component
public class ReconcileLoop {

    private static final Logger log = LoggerFactory.getLogger(ReconcileLoop.class);

    private final TractStatusRepository statuses;
    private final ReconciliationLog reconciliationLog;
    private final RetryPolicy retryPolicy;

    public ReconcileLoop(TractStatusRepository statuses,
                         ReconciliationLog reconciliationLog,
                         ReconcilerProperties props) {
        this.statuses = statuses;
        this.reconciliationLog = reconciliationLog;
        this.retryPolicy = new RetryPolicy(props);
    }

    public void reconcile(Tract tract) {
        TractStatus current = statuses.findByTract(tract.name())
                .orElseGet(() -> initial(tract));

        // Steps 1 - 5 from 2.2.3 (read pair, compute delta, plan, execute, update status)
        // are wired together by the ReconcilePlanner that lands in the
        // next commit. This loop only handles status bookkeeping and
        // delegates the work to ReconcilePlanner via the executor port
        // injected at runtime.
        log.debug("Tick for tract {} (desired={}, recon={})",
                tract.name(), tract.desiredState(), current.reconciliationStatus());

        TractStatus updated = new TractStatus(
                current.tract(),
                tract.desiredState(),
                ReconciliationStatus.RECONCILING,
                current.connector(),
                current.topics(),
                current.flinkJob(),
                current.sinks(),
                current.flinkJobId(),
                current.lastError(),
                tract.specVersion().value(),
                Instant.now()
        );
        statuses.save(updated);
    }

    private TractStatus initial(Tract tract) {
        return new TractStatus(
                tract.name(),
                tract.desiredState(),
                ReconciliationStatus.DRIFT,
                Optional.empty(),
                List.of(),
                Optional.empty(),
                List.of(),
                Optional.empty(),
                Optional.empty(),
                tract.specVersion().value(),
                Instant.now()
        );
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public ReconciliationLog reconciliationLog() {
        return reconciliationLog;
    }
}
