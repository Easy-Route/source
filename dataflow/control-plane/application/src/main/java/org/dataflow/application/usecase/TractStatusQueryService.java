package org.dataflow.application.usecase;

import org.dataflow.domain.port.repository.CommandLog;
import org.dataflow.domain.port.repository.ReconciliationLog;
import org.dataflow.domain.port.repository.TractStatusRepository;
import org.dataflow.domain.tract.TractName;
import org.dataflow.domain.tract.TractStatus;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class TractStatusQueryService {

    private final TractStatusRepository statuses;
    private final ReconciliationLog reconciliationLog;
    private final CommandLog commandLog;

    public TractStatusQueryService(TractStatusRepository statuses,
                                   ReconciliationLog reconciliationLog,
                                   CommandLog commandLog) {
        this.statuses = statuses;
        this.reconciliationLog = reconciliationLog;
        this.commandLog = commandLog;
    }

    public Optional<TractStatus> status(TractName tract) {
        return statuses.findByTract(tract);
    }

    public List<ReconciliationLog.Entry> reconciliationEvents(TractName tract, int limit) {
        return reconciliationLog.recent(tract, limit);
    }

    public List<CommandLog.Entry> commandHistory(TractName tract, int limit) {
        return commandLog.recent(tract, limit);
    }
}
