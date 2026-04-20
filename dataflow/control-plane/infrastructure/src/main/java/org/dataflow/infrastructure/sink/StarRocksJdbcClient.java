package org.dataflow.infrastructure.sink;

import org.dataflow.domain.port.dataplane.StarRocksClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class StarRocksJdbcClient implements StarRocksClient {

    private final String defaultJdbcUser;
    private final String defaultJdbcPassword;

    public StarRocksJdbcClient(
            @Value("${dataflow.starrocks.user:root}") String defaultJdbcUser,
            @Value("${dataflow.starrocks.password:}") String defaultJdbcPassword) {
        this.defaultJdbcUser = defaultJdbcUser;
        this.defaultJdbcPassword = defaultJdbcPassword;
    }

    @Override
    public boolean databaseExists(String database) {
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new StarRocksClientException("databaseExists failed", e);
        }
    }

    @Override
    public List<String> listTables(String database) {
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new StarRocksClientException("listTables failed", e);
        }
    }

    @Override
    public Optional<TableDescriptor> describeTable(String database, String table) {
        try (Connection c = open()) {
            List<Column> columns = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = ? AND table_name = ?
                    ORDER BY ordinal_position
                    """)) {
                ps.setString(1, database);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        columns.add(new Column(
                                rs.getString("column_name"),
                                rs.getString("data_type"),
                                "YES".equalsIgnoreCase(rs.getString("is_nullable"))
                        ));
                    }
                }
            }
            if (columns.isEmpty()) {
                return Optional.empty();
            }
            List<String> pk = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("""
                    SELECT column_name FROM information_schema.key_column_usage
                    WHERE table_schema = ? AND table_name = ?
                    ORDER BY ordinal_position
                    """)) {
                ps.setString(1, database);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        pk.add(rs.getString(1));
                    }
                }
            }
            String model = pk.isEmpty() ? "DUPLICATE" : "PRIMARY_KEY";
            return Optional.of(new TableDescriptor(database, table, columns, pk, model));
        } catch (SQLException e) {
            throw new StarRocksClientException("describeTable failed", e);
        }
    }

    @Override
    public void writeHeartbeat(String database, String tractName) {
        String sql = "INSERT INTO " + database + "._dataflow_heartbeat(tract_name, ts) VALUES (?, ?)";
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, tractName);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StarRocksClientException("writeHeartbeat failed", e);
        }
    }

    private Connection open() throws SQLException {
        // Real implementation would route through HikariCP per-sink, with
        // FE host/port pulled from SinkSpec.connection. The control plane
        // uses MySQL-protocol JDBC for introspection (StarRocks FE accepts
        // it). Stream Load itself is owned by the Flink connector.
        String url = "jdbc:mysql://starrocks-fe:9030/information_schema";
        return DriverManager.getConnection(url, defaultJdbcUser, defaultJdbcPassword);
    }

    public static class StarRocksClientException extends RuntimeException {
        public StarRocksClientException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
