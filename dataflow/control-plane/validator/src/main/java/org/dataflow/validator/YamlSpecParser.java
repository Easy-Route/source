package org.dataflow.validator;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.dataflow.domain.spec.TractSpec;
import org.springframework.stereotype.Component;

@Component
public class YamlSpecParser {

    private final ObjectMapper yaml;
    private final ObjectMapper json;
    private final SecretResolver secretResolver;

    public YamlSpecParser(SecretResolver secretResolver) {
        this.secretResolver = secretResolver;
        this.yaml = new ObjectMapper(new YAMLFactory())
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        this.json = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .addModule(new Jdk8Module())
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    public ParsedSpec parse(String yamlDocument) {
        String resolved = secretResolver.resolve(yamlDocument);
        try {
            JsonNode tree = yaml.readTree(resolved);
            TractSpec spec = json.treeToValue(tree, TractSpec.class);
            return new ParsedSpec(spec, tree);
        } catch (JsonProcessingException e) {
            JsonLocation loc = e.getLocation();
            throw new SpecParseException(
                    e.getOriginalMessage(),
                    loc != null ? loc.getLineNr() : null,
                    loc != null ? loc.getColumnNr() : null,
                    e);
        } catch (Exception e) {
            throw new SpecParseException(e.getMessage(), null, null, e);
        }
    }

    public ObjectMapper jsonMapper() {
        return json;
    }

    public record ParsedSpec(TractSpec spec, JsonNode tree) {
    }
}
