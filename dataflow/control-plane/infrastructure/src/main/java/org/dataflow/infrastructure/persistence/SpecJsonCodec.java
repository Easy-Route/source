package org.dataflow.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.dataflow.domain.spec.SourceSpec;
import org.dataflow.domain.spec.TractSpec;
import org.springframework.stereotype.Component;

@Component
public class SpecJsonCodec {

    private final ObjectMapper mapper;

    public SpecJsonCodec() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public String writeTractSpec(TractSpec spec) {
        try {
            return mapper.writeValueAsString(spec);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize TractSpec", e);
        }
    }

    public TractSpec readTractSpec(String json) {
        try {
            return mapper.readValue(json, TractSpec.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize TractSpec", e);
        }
    }

    public String writeSourceSpec(SourceSpec spec) {
        try {
            return mapper.writeValueAsString(spec);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize SourceSpec", e);
        }
    }

    public SourceSpec readSourceSpec(String json) {
        try {
            return mapper.readValue(json, SourceSpec.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize SourceSpec", e);
        }
    }
}
