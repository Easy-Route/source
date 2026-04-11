package org.dataflow.domain.spec;

import java.util.List;

public record SourceSpec(
        String type,
        ConnectionSpec connection,
        PublicationSpec publication,
        SnapshotSpec snapshot
) {
    public SourceSpec {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("source.type is required");
        }
        if (connection == null) {
            throw new IllegalArgumentException("source.connection is required");
        }
        if (publication == null) {
            throw new IllegalArgumentException("source.publication is required");
        }
    }

    public record ConnectionSpec(
            String host,
            int port,
            String database,
            String user,
            String password
    ) {
        public ConnectionSpec {
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("connection.host is required");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("connection.port out of range");
            }
        }
    }

    public record PublicationSpec(String name, List<String> tables) {
        public PublicationSpec {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("publication.name is required");
            }
            if (tables == null || tables.isEmpty()) {
                throw new IllegalArgumentException("publication.tables must be non-empty");
            }
            tables = List.copyOf(tables);
        }
    }

    public record SnapshotSpec(SnapshotMode mode) {
        public SnapshotSpec {
            if (mode == null) {
                mode = SnapshotMode.INITIAL;
            }
        }
    }

    public enum SnapshotMode {
        INITIAL, NEVER, WHEN_NEEDED
    }
}
