package org.dataflow.flink.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);

    private ConfigLoader() {
    }

    public static TractConfig load(Path file) {
        try {
            byte[] bytes = Files.readAllBytes(file);
            return YAML.readValue(bytes, TractConfig.class);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load tract config " + file, e);
        }
    }
}
