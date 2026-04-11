package org.dataflow.domain.spec;

import java.util.Map;

public record SinkSpec(
        String name,
        String type,
        SinkConnection connection,
        MappingSpec mapping
) {
    public SinkSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("sink.name is required");
        }
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("sink.type is required");
        }
        if (connection == null) {
            throw new IllegalArgumentException("sink.connection is required");
        }
        if (mapping == null) {
            throw new IllegalArgumentException("sink.mapping is required");
        }
    }

    public record SinkConnection(Map<String, String> properties) {
        public SinkConnection {
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }

        public String required(String key) {
            String value = properties.get(key);
            if (value == null) {
                throw new IllegalArgumentException("sink.connection." + key + " is required");
            }
            return value;
        }
    }

    public record MappingSpec(MappingMode mode, boolean primaryKeyModel, Map<String, String> tableOverrides) {
        public MappingSpec {
            if (mode == null) {
                mode = MappingMode.ONE_TO_ONE;
            }
            tableOverrides = tableOverrides == null ? Map.of() : Map.copyOf(tableOverrides);
        }
    }

    public enum MappingMode {
        ONE_TO_ONE
    }
}
