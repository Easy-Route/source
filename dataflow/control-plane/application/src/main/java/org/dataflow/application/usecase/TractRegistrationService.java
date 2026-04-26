package org.dataflow.application.usecase;

import org.dataflow.domain.port.repository.SourceRepository;
import org.dataflow.domain.port.repository.TractRepository;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.spec.TractSpec;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractName;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class TractRegistrationService {

    private final TractRepository tracts;
    private final SourceRepository sources;
    private final Clock clock;

    public TractRegistrationService(TractRepository tracts, SourceRepository sources, Clock clock) {
        this.tracts = tracts;
        this.sources = sources;
        this.clock = clock;
    }

    @Transactional
    public Tract create(TractSpec spec) {
        TractName name = new TractName(spec.name());
        if (tracts.existsByName(name)) {
            throw new TractAlreadyExistsException(name);
        }
        SourceName source = resolveSource(spec);
        Tract tract = Tract.draft(name, source, spec, Instant.now(clock));
        tracts.save(tract);
        return tract;
    }

    @Transactional
    public Tract update(TractName name, TractSpec spec) {
        Tract tract = tracts.findByName(name).orElseThrow(() -> new TractNotFoundException(name));
        tract.replaceSpec(spec, Instant.now(clock));
        tracts.save(tract);
        return tract;
    }

    public Optional<Tract> find(TractName name) {
        return tracts.findByName(name);
    }

    public List<Tract> list() {
        return tracts.findAll();
    }

    private SourceName resolveSource(TractSpec spec) {
        SourceName candidate = new SourceName(spec.source().connection().database());
        if (!sources.existsByName(candidate)) {
            throw new SourceNotRegisteredException(candidate);
        }
        return candidate;
    }

    public static class TractAlreadyExistsException extends RuntimeException {
        public TractAlreadyExistsException(TractName name) {
            super("Tract already exists: " + name);
        }
    }

    public static class TractNotFoundException extends RuntimeException {
        public TractNotFoundException(TractName name) {
            super("Tract not found: " + name);
        }
    }

    public static class SourceNotRegisteredException extends RuntimeException {
        public SourceNotRegisteredException(SourceName name) {
            super("Source not registered: " + name);
        }
    }
}
