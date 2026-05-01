package org.dataflow.flink.sink;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.dataflow.flink.config.TractConfig;
import org.dataflow.flink.event.CdcEvent;

import java.util.HashMap;
import java.util.Map;

public final class StarRocksSinkFactory {

    private StarRocksSinkFactory() {
    }

    public static DataStreamSink<String> attach(DataStream<CdcEvent> stream,
                                                TractConfig.Sink sink,
                                                String fallbackTable) {
        Map<String, String> conn = sink.connection();
        String database = required(conn, "database");
        String fePort = conn.getOrDefault("fePort", "8030");
        String feHost = required(conn, "feHost");
        String user = conn.getOrDefault("user", "root");
        String password = conn.getOrDefault("password", "");

        StarRocksSinkOptions opts = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://" + feHost + ":9030")
                .withProperty("load-url", feHost + ":" + fePort)
                .withProperty("database-name", database)
                .withProperty("table-name", fallbackTable)
                .withProperty("username", user)
                .withProperty("password", password)
                .withProperty("sink.buffer-flush.max-rows", "64000")
                .withProperty("sink.buffer-flush.max-bytes", "94371840")
                .withProperty("sink.buffer-flush.interval-ms", "5000")
                .withProperty("sink.max-retries", "3")
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        DataStream<String> rows = stream.map((MapFunction<CdcEvent, String>) JsonRowSerializer::serialize)
                .name("starrocks-row-" + sink.name());

        return rows.sinkTo(StarRocksSink.sink(opts))
                .name("starrocks-sink-" + sink.name());
    }

    private static String required(Map<String, String> map, String key) {
        String v = map.get(key);
        if (v == null) {
            throw new IllegalArgumentException("sink.connection." + key + " is required");
        }
        return v;
    }

    private static final class JsonRowSerializer {
        private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
                new com.fasterxml.jackson.databind.ObjectMapper();

        static String serialize(CdcEvent event) {
            try {
                Map<String, Object> row = new HashMap<>();
                if (event.after() != null) {
                    row.putAll(event.after());
                } else if (event.before() != null) {
                    row.putAll(event.before());
                }
                row.put("__op", event.op());
                row.put("__source_lsn", event.source().lsn());
                row.put("__source_ts_ms", event.source().tsMs());
                return MAPPER.writeValueAsString(row);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to serialize event for StarRocks", e);
            }
        }
    }
}
