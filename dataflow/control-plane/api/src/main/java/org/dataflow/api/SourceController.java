package org.dataflow.api;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.dataflow.application.usecase.SourceRegistrationService;
import org.dataflow.domain.source.Source;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.spec.SourceSpec;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/sources")
@Tag(name = "Sources", description = "Управление источниками данных")
public class SourceController {

    private final SourceRegistrationService sources;

    public SourceController(SourceRegistrationService sources) {
        this.sources = sources;
    }

    @GetMapping
    public List<SourceView> list() {
        return sources.list().stream().map(SourceView::from).toList();
    }

    @PostMapping("/{name}")
    public ResponseEntity<SourceView> create(@PathVariable String name, @RequestBody SourceSpec spec) {
        Source source = sources.register(new SourceName(name), spec);
        return ResponseEntity.status(HttpStatus.CREATED).body(SourceView.from(source));
    }

    @PutMapping("/{name}")
    public SourceView update(@PathVariable String name, @RequestBody SourceSpec spec) {
        return SourceView.from(sources.update(new SourceName(name), spec));
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable String name) {
        sources.delete(new SourceName(name));
        return ResponseEntity.noContent().build();
    }

    public record SourceView(String name, String type, Instant createdAt, Instant updatedAt) {
        static SourceView from(Source source) {
            return new SourceView(
                    source.name().value(),
                    source.spec().type(),
                    source.createdAt(),
                    source.updatedAt());
        }
    }
}
