package org.dataflow.infrastructure.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.dataflow.domain.tract.Tract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class TractConfigPublisher {

    private static final Logger log = LoggerFactory.getLogger(TractConfigPublisher.class);

    private final ObjectMapper yaml;
    private final Path baseDir;

    public TractConfigPublisher(@Value("${dataflow.flink.config-dir:/opt/flink/conf/tracts}") String dir) {
        this.yaml = new ObjectMapper(new YAMLFactory());
        this.baseDir = Path.of(dir);
    }

    public String publish(Tract tract) {
        Path file = baseDir.resolve(tract.name().value() + ".yaml");
        try {
            Files.createDirectories(baseDir);
            yaml.writeValue(file.toFile(), tract.spec());
            log.info("Published tract config: {}", file);
            return file.toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write tract config " + file, e);
        }
    }
}
