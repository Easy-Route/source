package org.dataflow.validator;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class SchemaValidator {

    private static final String SCHEMA_RESOURCE = "schemas/tract-spec.schema.json";

    private final JsonSchema schema;

    public SchemaValidator() {
        try (InputStream in = new ClassPathResource(SCHEMA_RESOURCE).getInputStream()) {
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
            this.schema = factory.getSchema(in);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load JSON schema " + SCHEMA_RESOURCE, e);
        }
    }

    public ValidationResult validate(JsonNode document) {
        Set<ValidationMessage> messages = schema.validate(document);
        if (messages.isEmpty()) {
            return ValidationResult.ok();
        }
        List<ValidationError> errors = new ArrayList<>(messages.size());
        for (ValidationMessage msg : messages) {
            errors.add(new ValidationError(msg.getInstanceLocation().toString(), msg.getMessage()));
        }
        return ValidationResult.of(errors);
    }
}
