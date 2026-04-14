package org.dataflow.infrastructure.persistence;

import org.dataflow.domain.port.repository.CommandLog;
import org.dataflow.domain.tract.TractName;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class JdbcCommandLog implements CommandLog {

    private final JdbcTemplate jdbc;

    public JdbcCommandLog(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public UUID record(TractName tract, String command, String issuedBy, String payload) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO tract_command_log(id, tract_name, command, issued_by, payload)
                VALUES (?, ?, ?, ?, ?::jsonb)
                """, id, tract.value(), command, issuedBy, payload);
        return id;
    }

    @Override
    public List<Entry> recent(TractName tract, int limit) {
        return jdbc.query("""
                SELECT id, tract_name, command, issued_by, payload, issued_at
                FROM tract_command_log
                WHERE tract_name = ?
                ORDER BY issued_at DESC
                LIMIT ?
                """, (rs, i) -> new Entry(
                        (UUID) rs.getObject("id"),
                        new TractName(rs.getString("tract_name")),
                        rs.getString("command"),
                        rs.getString("issued_by"),
                        rs.getString("payload"),
                        rs.getTimestamp("issued_at").toInstant()
                ), tract.value(), limit);
    }
}
