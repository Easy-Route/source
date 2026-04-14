package org.dataflow.infrastructure.persistence;

import org.dataflow.domain.port.repository.ReconciliationLog;
import org.dataflow.domain.tract.TractName;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class JdbcReconciliationLog implements ReconciliationLog {

    private final JdbcTemplate jdbc;

    public JdbcReconciliationLog(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public UUID append(TractName tract,
                       String stepName,
                       Outcome outcome,
                       String detail,
                       long durationMillis) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO reconciliation_log(id, tract_name, step_name, outcome, detail, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """, id, tract.value(), stepName, outcome.name(), detail, durationMillis);
        return id;
    }

    @Override
    public List<Entry> recent(TractName tract, int limit) {
        return jdbc.query("""
                SELECT id, tract_name, step_name, outcome, detail, duration_ms, recorded_at
                FROM reconciliation_log
                WHERE tract_name = ?
                ORDER BY recorded_at DESC
                LIMIT ?
                """, (rs, i) -> new Entry(
                        (UUID) rs.getObject("id"),
                        new TractName(rs.getString("tract_name")),
                        rs.getString("step_name"),
                        Outcome.valueOf(rs.getString("outcome")),
                        rs.getString("detail"),
                        rs.getLong("duration_ms"),
                        rs.getTimestamp("recorded_at").toInstant()
                ), tract.value(), limit);
    }
}
