package org.dataflow.domain.port.dataplane;

import org.dataflow.domain.spec.SourceSpec;

import java.util.List;

public interface SourceProbe {

    boolean publicationExists(SourceSpec.ConnectionSpec conn, String publicationName);

    boolean tableExists(SourceSpec.ConnectionSpec conn, String schema, String table);

    boolean userHasReplicationRole(SourceSpec.ConnectionSpec conn, String username);

    boolean userHasSelectOn(SourceSpec.ConnectionSpec conn, String username, String schema, String table);

    List<ColumnInfo> describeColumns(SourceSpec.ConnectionSpec conn, String schema, String table);

    record ColumnInfo(String name, String dataType, boolean nullable) {
    }
}
