package org.dataflow.infrastructure.persistence;

import org.dataflow.domain.port.repository.TractRepository;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.spec.TractSpec;
import org.dataflow.domain.tract.DesiredState;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractName;
import org.dataflow.domain.tract.TractSpecVersion;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcTractRepository implements TractRepository {

    private final JdbcTemplate jdbc;
    private final SpecJsonCodec codec;

    public JdbcTractRepository(JdbcTemplate jdbc, SpecJsonCodec codec) {
        this.jdbc = jdbc;
        this.codec = codec;
    }

    @Override
    public Optional<Tract> findByName(TractName name) {
        try {
            Tract tract = jdbc.queryForObject("""
                    SELECT t.name, t.source_name, t.desired_state, t.current_version,
                           t.created_at, t.updated_at, s.spec, s.raw_yaml
                    FROM tract t
                    JOIN tract_spec s ON s.tract_name = t.name AND s.version = t.current_version
                    WHERE t.name = ?
                    """, this::mapRow, name.value());
            return Optional.ofNullable(tract);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Tract> findAll() {
        return jdbc.query("""
                SELECT t.name, t.source_name, t.desired_state, t.current_version,
                       t.created_at, t.updated_at, s.spec, s.raw_yaml
                FROM tract t
                JOIN tract_spec s ON s.tract_name = t.name AND s.version = t.current_version
                ORDER BY t.name
                """, this::mapRow);
    }

    @Override
    public List<Tract> findBySource(SourceName source) {
        return jdbc.query("""
                SELECT t.name, t.source_name, t.desired_state, t.current_version,
                       t.created_at, t.updated_at, s.spec, s.raw_yaml
                FROM tract t
                JOIN tract_spec s ON s.tract_name = t.name AND s.version = t.current_version
                WHERE t.source_name = ?
                ORDER BY t.name
                """, this::mapRow, source.value());
    }

    @Override
    public List<Tract> findRequiringReconciliation() {
        return jdbc.query("""
                SELECT t.name, t.source_name, t.desired_state, t.current_version,
                       t.created_at, t.updated_at, s.spec, s.raw_yaml
                FROM tract t
                JOIN tract_spec s ON s.tract_name = t.name AND s.version = t.current_version
                WHERE t.desired_state IN ('DEPLOYED', 'SUSPENDED', 'DELETED')
                """, this::mapRow);
    }

    @Override
    @Transactional
    public void save(Tract tract) {
        String specJson = codec.writeTractSpec(tract.spec());
        jdbc.update("""
                INSERT INTO tract(name, source_name, desired_state, current_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (name) DO UPDATE
                SET desired_state = EXCLUDED.desired_state,
                    current_version = EXCLUDED.current_version,
                    updated_at = EXCLUDED.updated_at
                """,
                tract.name().value(),
                tract.source().value(),
                tract.desiredState().name(),
                tract.specVersion().value(),
                Timestamp.from(tract.createdAt()),
                Timestamp.from(tract.updatedAt())
        );
        jdbc.update("""
                INSERT INTO tract_spec(tract_name, version, spec, raw_yaml)
                VALUES (?, ?, ?::jsonb, ?)
                ON CONFLICT (tract_name, version) DO UPDATE SET spec = EXCLUDED.spec
                """,
                tract.name().value(),
                tract.specVersion().value(),
                specJson,
                tract.spec().rawDocument()
        );
    }

    @Override
    public void delete(TractName name) {
        jdbc.update("DELETE FROM tract WHERE name = ?", name.value());
    }

    @Override
    public boolean existsByName(TractName name) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(1) FROM tract WHERE name = ?",
                Integer.class, name.value());
        return count != null && count > 0;
    }

    private Tract mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        TractName name = new TractName(rs.getString("name"));
        SourceName source = new SourceName(rs.getString("source_name"));
        DesiredState state = DesiredState.valueOf(rs.getString("desired_state"));
        TractSpec spec = codec.readTractSpec(rs.getString("spec"));
        TractSpecVersion version = new TractSpecVersion(rs.getLong("current_version"));
        Instant createdAt = rs.getTimestamp("created_at").toInstant();
        Instant updatedAt = rs.getTimestamp("updated_at").toInstant();
        return Tract.restore(name, source, state, spec, version, createdAt, updatedAt);
    }
}
