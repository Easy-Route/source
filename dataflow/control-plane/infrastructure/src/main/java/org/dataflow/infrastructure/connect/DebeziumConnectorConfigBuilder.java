package org.dataflow.infrastructure.connect;

import org.dataflow.domain.source.Source;
import org.dataflow.domain.spec.SourceSpec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class DebeziumConnectorConfigBuilder {

    private DebeziumConnectorConfigBuilder() {
    }

    public static String connectorNameFor(Source source) {
        return "dbz-" + source.name().value();
    }

    public static Map<String, String> buildPostgresConfig(Source source) {
        SourceSpec spec = source.spec();
        SourceSpec.ConnectionSpec conn = spec.connection();

        Map<String, String> cfg = new LinkedHashMap<>();
        cfg.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        cfg.put("plugin.name", "pgoutput");
        cfg.put("database.hostname", conn.host());
        cfg.put("database.port", Integer.toString(conn.port()));
        cfg.put("database.user", conn.user());
        cfg.put("database.password", conn.password());
        cfg.put("database.dbname", conn.database());
        cfg.put("database.server.name", source.name().value());
        cfg.put("topic.prefix", source.name().value());

        cfg.put("publication.name", spec.publication().name());
        cfg.put("publication.autocreate.mode", "filtered");
        cfg.put("table.include.list", spec.publication().tables().stream()
                .collect(Collectors.joining(",")));
        cfg.put("slot.name", "dbz_" + source.name().value().replace('-', '_'));

        cfg.put("snapshot.mode", switch (spec.snapshot().mode()) {
            case INITIAL -> "initial";
            case NEVER -> "never";
            case WHEN_NEEDED -> "when_needed";
        });

        cfg.put("tombstones.on.delete", "false");
        cfg.put("decimal.handling.mode", "string");
        cfg.put("time.precision.mode", "adaptive");
        cfg.put("heartbeat.interval.ms", "10000");

        return cfg;
    }
}
