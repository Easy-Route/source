package org.dataflow.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.dataflow.api.dto.SpecRequest;
import org.dataflow.api.dto.TractDto;
import org.dataflow.api.dto.TractStatusDto;
import org.dataflow.application.usecase.TractCommandService;
import org.dataflow.application.usecase.TractRegistrationService;
import org.dataflow.application.usecase.TractStatusQueryService;
import org.dataflow.domain.spec.TractSpec;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractName;
import org.dataflow.validator.SchemaValidator;
import org.dataflow.validator.SemanticValidator;
import org.dataflow.validator.ValidationException;
import org.dataflow.validator.ValidationResult;
import org.dataflow.validator.YamlSpecParser;
import org.dataflow.validator.diff.DiffClassifier;
import org.dataflow.domain.tract.ChangeImpact;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/tracts")
@Tag(name = "Tracts", description = "Управление трактами")
public class TractController {

    private final TractRegistrationService registration;
    private final TractCommandService commands;
    private final TractStatusQueryService statuses;
    private final YamlSpecParser parser;
    private final SchemaValidator schemaValidator;
    private final SemanticValidator semanticValidator;
    private final DiffClassifier diffClassifier;

    public TractController(TractRegistrationService registration,
                           TractCommandService commands,
                           TractStatusQueryService statuses,
                           YamlSpecParser parser,
                           SchemaValidator schemaValidator,
                           SemanticValidator semanticValidator,
                           DiffClassifier diffClassifier) {
        this.registration = registration;
        this.commands = commands;
        this.statuses = statuses;
        this.parser = parser;
        this.schemaValidator = schemaValidator;
        this.semanticValidator = semanticValidator;
        this.diffClassifier = diffClassifier;
    }

    @GetMapping
    @Operation(summary = "Перечень всех трактов")
    public List<TractDto> list() {
        return registration.list().stream().map(TractDto::from).toList();
    }

    @PostMapping
    @Operation(summary = "Создание тракта по спецификации")
    public ResponseEntity<TractDto> create(@RequestBody @Valid SpecRequest body) {
        TractSpec spec = parseAndValidate(body);
        Tract tract = registration.create(spec);
        return ResponseEntity.status(HttpStatus.CREATED).body(TractDto.from(tract));
    }

    @GetMapping("/{name}")
    @Operation(summary = "Получение спецификации тракта")
    public TractDto get(@PathVariable String name) {
        return registration.find(new TractName(name))
                .map(TractDto::from)
                .orElseThrow(() -> new TractRegistrationService.TractNotFoundException(new TractName(name)));
    }

    @PutMapping("/{name}")
    @Operation(summary = "Обновление спецификации тракта")
    public TractDto update(@PathVariable String name,
                           @RequestParam(defaultValue = "false") boolean allowRestart,
                           @RequestBody @Valid SpecRequest body) {
        TractName tractName = new TractName(name);
        Tract existing = registration.find(tractName)
                .orElseThrow(() -> new TractRegistrationService.TractNotFoundException(tractName));
        TractSpec newSpec = parseAndValidate(body);
        DiffClassifier.Classification classification = diffClassifier.classify(existing.spec(), newSpec);
        if (classification.impact() == ChangeImpact.REQUIRES_RECREATE) {
            throw new ValidationException(ValidationResult.of(List.of(
                    new org.dataflow.validator.ValidationError(
                            "spec",
                            "Change requires recreating the tract; create a new tract and delete the old one"))));
        }
        if (classification.impact() == ChangeImpact.REQUIRES_RESTART && !allowRestart) {
            throw new RestartRequiredException();
        }
        return TractDto.from(registration.update(tractName, newSpec));
    }

    @DeleteMapping("/{name}")
    @Operation(summary = "Удаление тракта")
    public ResponseEntity<Void> delete(@PathVariable String name,
                                       @RequestHeader(value = "X-Issuer", defaultValue = "anonymous") String issuer) {
        commands.delete(new TractName(name), issuer);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{name}/deploy")
    @Operation(summary = "Команда развёртывания")
    public ResponseEntity<Map<String, String>> deploy(@PathVariable String name,
                                                      @RequestHeader(value = "X-Issuer", defaultValue = "anonymous") String issuer) {
        commands.deploy(new TractName(name), issuer);
        return ResponseEntity.accepted().body(Map.of("status", "scheduled"));
    }

    @PostMapping("/{name}/suspend")
    @Operation(summary = "Команда приостановки")
    public ResponseEntity<Map<String, String>> suspend(@PathVariable String name,
                                                       @RequestHeader(value = "X-Issuer", defaultValue = "anonymous") String issuer) {
        commands.suspend(new TractName(name), issuer);
        return ResponseEntity.accepted().body(Map.of("status", "scheduled"));
    }

    @PostMapping("/{name}/resume")
    @Operation(summary = "Команда возобновления")
    public ResponseEntity<Map<String, String>> resume(@PathVariable String name,
                                                      @RequestHeader(value = "X-Issuer", defaultValue = "anonymous") String issuer) {
        commands.resume(new TractName(name), issuer);
        return ResponseEntity.accepted().body(Map.of("status", "scheduled"));
    }

    @GetMapping("/{name}/status")
    @Operation(summary = "Наблюдаемое состояние тракта")
    public TractStatusDto status(@PathVariable String name) {
        return statuses.status(new TractName(name))
                .map(TractStatusDto::from)
                .orElseGet(() -> TractStatusDto.unknown(name));
    }

    @GetMapping("/{name}/events")
    @Operation(summary = "Журнал событий тракта")
    public List<?> events(@PathVariable String name,
                          @RequestParam(defaultValue = "100") int limit) {
        return statuses.reconciliationEvents(new TractName(name), limit);
    }

    @GetMapping("/{name}/dlq")
    @Operation(summary = "Содержимое DLQ-топика тракта")
    public List<?> dlq(@PathVariable String name,
                       @RequestParam(defaultValue = "100") int limit) {
        // Real implementation goes through DlqReader port; not exposed here
        // because reading raw Kafka messages is the operator's path, not the
        // synchronous request path.
        return List.of();
    }

    private TractSpec parseAndValidate(SpecRequest body) {
        var parsed = parser.parse(body.document());
        ValidationResult schema = schemaValidator.validate(parsed.tree());
        if (!schema.isValid()) {
            throw new ValidationException(schema);
        }
        ValidationResult semantic = semanticValidator.validate(parsed.spec());
        if (!semantic.isValid()) {
            throw new ValidationException(semantic);
        }
        return parsed.spec();
    }

    public static class RestartRequiredException extends RuntimeException {
        public RestartRequiredException() {
            super("Change requires Flink savepoint restart; pass ?allowRestart=true to confirm");
        }
    }
}
