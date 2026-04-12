package org.dataflow.domain.port.repository;

import org.dataflow.domain.tract.TractName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface CommandLog {

    UUID record(TractName tract, String command, String issuedBy, String payload);

    List<Entry> recent(TractName tract, int limit);

    record Entry(UUID id, TractName tract, String command, String issuedBy, String payload, Instant at) {
    }
}
