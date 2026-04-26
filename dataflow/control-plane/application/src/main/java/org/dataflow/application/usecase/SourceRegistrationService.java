package org.dataflow.application.usecase;

import org.dataflow.domain.port.repository.SourceRepository;
import org.dataflow.domain.source.Source;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.spec.SourceSpec;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class SourceRegistrationService {

    private final SourceRepository sources;
    private final Clock clock;

    public SourceRegistrationService(SourceRepository sources, Clock clock) {
        this.sources = sources;
        this.clock = clock;
    }

    @Transactional
    public Source register(SourceName name, SourceSpec spec) {
        if (sources.existsByName(name)) {
            throw new IllegalStateException("Source already exists: " + name);
        }
        Source source = Source.register(name, spec, Instant.now(clock));
        sources.save(source);
        return source;
    }

    @Transactional
    public Source update(SourceName name, SourceSpec spec) {
        Source source = sources.findByName(name)
                .orElseThrow(() -> new IllegalStateException("Source not found: " + name));
        source.replaceSpec(spec, Instant.now(clock));
        sources.save(source);
        return source;
    }

    @Transactional
    public void delete(SourceName name) {
        long inUse = sources.countTractsReferencing(name);
        if (inUse > 0) {
            throw new IllegalStateException("Cannot delete source " + name
                    + ": still used by " + inUse + " tract(s)");
        }
        sources.delete(name);
    }

    public Optional<Source> find(SourceName name) {
        return sources.findByName(name);
    }

    public List<Source> list() {
        return sources.findAll();
    }
}
