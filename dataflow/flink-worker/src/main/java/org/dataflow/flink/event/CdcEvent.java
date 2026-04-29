package org.dataflow.flink.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CdcEvent(
        String op,
        Map<String, Object> before,
        Map<String, Object> after,
        Source source,
        long tsMs
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Source(
            String version,
            String connector,
            String name,
            long lsn,
            long txId,
            long sequence,
            String db,
            String schema,
            String table,
            long tsMs
    ) {
    }

    public String tableFqn() {
        return source.schema() + "." + source.table();
    }

    public String dedupKey() {
        return source.lsn() + ":" + source.txId() + ":" + source.sequence();
    }
}
