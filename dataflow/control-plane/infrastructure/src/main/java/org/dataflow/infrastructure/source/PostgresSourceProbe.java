package org.dataflow.infrastructure.source;

import org.dataflow.domain.port.dataplane.SourceProbe;
import org.dataflow.domain.spec.SourceSpec;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Component
public class PostgresSourceProbe implements SourceProbe {

    @Override
    public boolean publicationExists(SourceSpec.ConnectionSpec conn, String publicationName) {
        try (Connection c = open(conn);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT 1 FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new SourceProbeException("publicationExists failed", e);
        }
    }

    @Override
    public boolean tableExists(SourceSpec.ConnectionSpec conn, String schema, String table) {
        try (Connection c = open(conn);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new SourceProbeException("tableExists failed", e);
        }
    }

    @Override
    public boolean userHasReplicationRole(SourceSpec.ConnectionSpec conn, String username) {
        try (Connection c = open(conn);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT rolreplication FROM pg_authid WHERE rolname = ?")) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new SourceProbeException("userHasReplicationRole failed", e);
        }
    }

    @Override
    public boolean userHasSelectOn(SourceSpec.ConnectionSpec conn, String username, String schema, String table) {
        String fqn = schema + "." + table;
        try (Connection c = open(conn);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT has_table_privilege(?, ?, 'SELECT')")) {
            ps.setString(1, username);
            ps.setString(2, fqn);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new SourceProbeException("userHasSelectOn failed", e);
        }
    }

    @Override
    public List<ColumnInfo> describeColumns(SourceSpec.ConnectionSpec conn, String schema, String table) {
        try (Connection c = open(conn);
             PreparedStatement ps = c.prepareStatement("""
                     SELECT column_name, data_type, is_nullable
                     FROM information_schema.columns
                     WHERE table_schema = ? AND table_name = ?
                     ORDER BY ordinal_position
                     """)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                List<ColumnInfo> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new ColumnInfo(
                            rs.getString("column_name"),
                            rs.getString("data_type"),
                            "YES".equalsIgnoreCase(rs.getString("is_nullable"))
                    ));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new SourceProbeException("describeColumns failed", e);
        }
    }

    private Connection open(SourceSpec.ConnectionSpec conn) throws SQLException {
        String url = "jdbc:postgresql://" + conn.host() + ":" + conn.port() + "/" + conn.database();
        return DriverManager.getConnection(url, conn.user(), conn.password());
    }

    public static class SourceProbeException extends RuntimeException {
        public SourceProbeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
