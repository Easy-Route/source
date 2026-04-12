package org.dataflow.domain.port.dataplane;

import java.util.List;
import java.util.Optional;

public interface StarRocksClient {

    boolean databaseExists(String database);

    List<String> listTables(String database);

    Optional<TableDescriptor> describeTable(String database, String table);

    void writeHeartbeat(String database, String tractName);

    record TableDescriptor(
            String database,
            String table,
            List<Column> columns,
            List<String> primaryKeyColumns,
            String model
    ) {
    }

    record Column(String name, String type, boolean nullable) {
    }
}
