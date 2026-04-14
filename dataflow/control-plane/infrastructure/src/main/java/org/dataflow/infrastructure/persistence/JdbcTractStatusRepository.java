package org.dataflow.infrastructure.persistence;

import org.dataflow.domain.port.repository.TractStatusRepository;
import org.dataflow.domain.tract.DesiredState;
import org.dataflow.domain.tract.ReconciliationStatus;
import org.dataflow.domain.tract.TractName;
import org.dataflow.domain.tract.TractStatus;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcTractStatusRepository implements TractStatusRepository {

    private final JdbcTemplate jdbc;

    public JdbcTractStatusRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<TractStatus> findByTract(TractName name) {
        try {
            TractStatus status = jdbc.queryForObject("""
                    SELECT tract_name, desired_state, reconciliation_status, flink_job_id,
                           last_error, observed_spec_version, observed_at
                    FROM tract_status WHERE tract_name = ?
                    """, this::mapRow, name.value());
            return Optional.ofNullable(status);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public void save(TractStatus status) {
        jdbc.update("""
                INSERT INTO tract_status(tract_name, desired_state, reconciliation_status,
                                         flink_job_id, last_error, observed_spec_version, observed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tract_name) DO UPDATE
                SET desired_state = EXCLUDED.desired_state,
                    reconciliation_status = EXCLUDED.reconciliation_status,
                    flink_job_id = EXCLUDED.flink_job_id,
                    last_error = EXCLUDED.last_error,
                    observed_spec_version = EXCLUDED.observed_spec_version,
                    observed_at = EXCLUDED.observed_at
                """,
                status.tract().value(),
                status.desiredState().name(),
                status.reconciliationStatus().name(),
                status.flinkJobId().orElse(null),
                status.lastError().orElse(null),
                status.observedSpecVersion(),
                Timestamp.from(status.observedAt())
        );
    }

    @Override
    public void deleteByTract(TractName name) {
        jdbc.update("DELETE FROM tract_status WHERE tract_name = ?", name.value());
    }

    private TractStatus mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        TractName name = new TractName(rs.getString("tract_name"));
        DesiredState desired = DesiredState.valueOf(rs.getString("desired_state"));
        ReconciliationStatus recon = ReconciliationStatus.valueOf(rs.getString("reconciliation_status"));
        String flinkJobId = rs.getString("flink_job_id");
        String lastError = rs.getString("last_error");
        long observedVersion = rs.getLong("observed_spec_version");
        Instant observedAt = rs.getTimestamp("observed_at").toInstant();
        return new TractStatus(
                name, desired, recon,
                Optional.empty(), List.of(), Optional.empty(), List.of(),
                Optional.ofNullable(flinkJobId),
                Optional.ofNullable(lastError),
                observedVersion, observedAt);
    }
}
