package org.dataflow.infrastructure.persistence;

import org.dataflow.domain.port.repository.SourceRepository;
import org.dataflow.domain.source.Source;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.spec.SourceSpec;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcSourceRepository implements SourceRepository {

    private final JdbcTemplate jdbc;
    private final SpecJsonCodec codec;

    public JdbcSourceRepository(JdbcTemplate jdbc, SpecJsonCodec codec) {
        this.jdbc = jdbc;
        this.codec = codec;
    }

    @Override
    public Optional<Source> findByName(SourceName name) {
        return jdbc.query(
                "SELECT name, type, spec, created_at, updated_at FROM source WHERE name = ?",
                this::mapRow,
                name.value()
        ).stream().findFirst();
    }

    @Override
    public List<Source> findAll() {
        return jdbc.query(
                "SELECT name, type, spec, created_at, updated_at FROM source ORDER BY name",
                this::mapRow
        );
    }

    @Override
    public void save(Source source) {
        SourceSpec spec = source.spec();
        String specJson = codec.writeSourceSpec(spec);
        jdbc.update("""
                INSERT INTO source(name, type, spec, created_at, updated_at)
                VALUES (?, ?, ?::jsonb, ?, ?)
                ON CONFLICT (name) DO UPDATE
                SET type = EXCLUDED.type,
                    spec = EXCLUDED.spec,
                    updated_at = EXCLUDED.updated_at
                """,
                source.name().value(),
                spec.type(),
                specJson,
                Timestamp.from(source.createdAt()),
                Timestamp.from(source.updatedAt())
        );
    }

    @Override
    public void delete(SourceName name) {
        jdbc.update("DELETE FROM source WHERE name = ?", name.value());
    }

    @Override
    public boolean existsByName(SourceName name) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(1) FROM source WHERE name = ?",
                Integer.class, name.value());
        return count != null && count > 0;
    }

    @Override
    public long countTractsReferencing(SourceName name) {
        Long count = jdbc.queryForObject(
                "SELECT COUNT(1) FROM tract WHERE source_name = ?",
                Long.class, name.value());
        return count == null ? 0 : count;
    }

    private Source mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        SourceSpec spec = codec.readSourceSpec(rs.getString("spec"));
        Instant createdAt = rs.getTimestamp("created_at").toInstant();
        Instant updatedAt = rs.getTimestamp("updated_at").toInstant();
        return Source.restore(new SourceName(rs.getString("name")), spec, createdAt, updatedAt);
    }
}
