package org.dataflow.domain.port.repository;

import org.dataflow.domain.tract.TractName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ReconciliationLog {

    UUID append(TractName tract,
                String stepName,
                Outcome outcome,
                String detail,
                long durationMillis);

    List<Entry> recent(TractName tract, int limit);

    enum Outcome {
        STARTED, SUCCEEDED, FAILED, SKIPPED
    }

    record Entry(
            UUID id,
            TractName tract,
            String stepName,
            Outcome outcome,
            String detail,
            long durationMillis,
            Instant at
    ) {
    }
}
