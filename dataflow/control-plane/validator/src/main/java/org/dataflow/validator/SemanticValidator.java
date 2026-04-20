package org.dataflow.validator;

import org.dataflow.domain.port.dataplane.SourceProbe;
import org.dataflow.domain.port.dataplane.StarRocksClient;
import org.dataflow.domain.spec.SinkSpec;
import org.dataflow.domain.spec.SourceSpec;
import org.dataflow.domain.spec.TractSpec;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component
public class SemanticValidator {

    private final SourceProbe source;
    private final StarRocksClient starRocks;

    public SemanticValidator(SourceProbe source, StarRocksClient starRocks) {
        this.source = source;
        this.starRocks = starRocks;
    }

    public ValidationResult validate(TractSpec spec) {
        List<ValidationError> errors = new ArrayList<>();
        validateSource(spec, errors);
        validateSinks(spec, errors);
        validateConnectivity(spec, errors);
        return ValidationResult.of(errors);
    }

    private void validateSource(TractSpec spec, List<ValidationError> errors) {
        SourceSpec.ConnectionSpec conn = spec.source().connection();
        String publication = spec.source().publication().name();

        if (!source.publicationExists(conn, publication)) {
            errors.add(new ValidationError(
                    "spec.source.publication.name",
                    "Publication " + publication + " does not exist on source"));
            return;
        }
        if (!source.userHasReplicationRole(conn, conn.user())) {
            errors.add(new ValidationError(
                    "spec.source.connection.user",
                    "User " + conn.user() + " does not have rolreplication = true"));
        }
        for (String fqn : spec.source().publication().tables()) {
            String[] parts = fqn.split("\\.", 2);
            if (parts.length != 2) {
                errors.add(new ValidationError(
                        "spec.source.publication.tables", "Bad table FQN: " + fqn));
                continue;
            }
            if (!source.tableExists(conn, parts[0], parts[1])) {
                errors.add(new ValidationError(
                        "spec.source.publication.tables",
                        "Table " + fqn + " does not exist on source"));
            } else if (!source.userHasSelectOn(conn, conn.user(), parts[0], parts[1])) {
                errors.add(new ValidationError(
                        "spec.source.publication.tables",
                        "User " + conn.user() + " has no SELECT on " + fqn));
            }
        }
    }

    private void validateSinks(TractSpec spec, List<ValidationError> errors) {
        for (SinkSpec sink : spec.sinks()) {
            String database = sink.connection().required("database");
            if (!starRocks.databaseExists(database)) {
                errors.add(new ValidationError(
                        "spec.sinks[" + sink.name() + "].connection.database",
                        "StarRocks database " + database + " does not exist"));
                continue;
            }
            if (sink.mapping().primaryKeyModel()) {
                Set<String> sinkTables = Set.copyOf(starRocks.listTables(database));
                for (String fqn : spec.source().publication().tables()) {
                    String tname = fqn.contains(".") ? fqn.substring(fqn.indexOf('.') + 1) : fqn;
                    String mapped = sink.mapping().tableOverrides().getOrDefault(fqn, tname);
                    if (!sinkTables.contains(mapped)) {
                        errors.add(new ValidationError(
                                "spec.sinks[" + sink.name() + "].mapping",
                                "Sink table " + database + "." + mapped + " does not exist"));
                        continue;
                    }
                    starRocks.describeTable(database, mapped).ifPresent(desc -> {
                        if (!"PRIMARY_KEY".equals(desc.model()) && sink.mapping().primaryKeyModel()) {
                            errors.add(new ValidationError(
                                    "spec.sinks[" + sink.name() + "].mapping.primaryKeyModel",
                                    "Table " + database + "." + mapped + " is not Primary Key Model"));
                        }
                    });
                }
            }
        }
    }

    private void validateConnectivity(TractSpec spec, List<ValidationError> errors) {
        Set<String> declaredTables = Set.copyOf(spec.source().publication().tables());
        for (SinkSpec sink : spec.sinks()) {
            for (String mappingKey : sink.mapping().tableOverrides().keySet()) {
                if (!declaredTables.contains(mappingKey)) {
                    errors.add(new ValidationError(
                            "spec.sinks[" + sink.name() + "].mapping.tableOverrides",
                            "Mapping references " + mappingKey
                                    + " which is not in source.publication.tables"));
                }
            }
        }
    }
}
