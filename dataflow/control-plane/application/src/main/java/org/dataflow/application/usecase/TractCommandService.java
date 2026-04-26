package org.dataflow.application.usecase;

import org.dataflow.domain.port.repository.CommandLog;
import org.dataflow.domain.port.repository.TractRepository;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractName;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

@Service
public class TractCommandService {

    private final TractRepository tracts;
    private final CommandLog commandLog;
    private final Clock clock;

    public TractCommandService(TractRepository tracts, CommandLog commandLog, Clock clock) {
        this.tracts = tracts;
        this.commandLog = commandLog;
        this.clock = clock;
    }

    @Transactional
    public Tract deploy(TractName name, String issuer) {
        return apply(name, "deploy", issuer, Tract::deploy);
    }

    @Transactional
    public Tract suspend(TractName name, String issuer) {
        return apply(name, "suspend", issuer, Tract::suspend);
    }

    @Transactional
    public Tract resume(TractName name, String issuer) {
        return apply(name, "resume", issuer, Tract::resume);
    }

    @Transactional
    public Tract delete(TractName name, String issuer) {
        return apply(name, "delete", issuer, Tract::delete);
    }

    private Tract apply(TractName name, String command, String issuer, TimedAction action) {
        Tract tract = tracts.findByName(name)
                .orElseThrow(() -> new TractRegistrationService.TractNotFoundException(name));
        Instant now = Instant.now(clock);
        action.apply(tract, now);
        tracts.save(tract);
        commandLog.record(name, command, issuer, "{}");
        return tract;
    }

    @FunctionalInterface
    private interface TimedAction {
        void apply(Tract tract, Instant now);
    }
}
