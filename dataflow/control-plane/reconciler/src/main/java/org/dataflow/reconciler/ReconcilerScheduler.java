package org.dataflow.reconciler;

import org.dataflow.domain.port.repository.TractRepository;
import org.dataflow.domain.tract.Tract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@EnableConfigurationProperties(ReconcilerProperties.class)
public class ReconcilerScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReconcilerScheduler.class);

    private final TractRepository tracts;
    private final ReconcileLoop loop;
    private final AdvisoryLockManager locks;

    public ReconcilerScheduler(TractRepository tracts,
                               ReconcileLoop loop,
                               AdvisoryLockManager locks) {
        this.tracts = tracts;
        this.loop = loop;
        this.locks = locks;
    }

    @Scheduled(fixedDelayString = "${dataflow.reconciler.period:PT10S}")
    public void tick() {
        List<Tract> candidates = tracts.findRequiringReconciliation();
        if (candidates.isEmpty()) {
            return;
        }
        log.debug("Reconciliation tick: {} tracts to evaluate", candidates.size());
        for (Tract tract : candidates) {
            if (!locks.tryLock(tract.name())) {
                log.debug("Skipping {} this tick — held by another instance", tract.name());
                continue;
            }
            try {
                loop.reconcile(tract);
            } catch (RuntimeException e) {
                log.warn("Reconciliation failed for {}: {}", tract.name(), e.toString());
            } finally {
                locks.unlock(tract.name());
            }
        }
    }
}
