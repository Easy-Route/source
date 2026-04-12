package org.dataflow.domain.port.repository;

import org.dataflow.domain.tract.TractName;
import org.dataflow.domain.tract.TractStatus;

import java.util.Optional;

public interface TractStatusRepository {

    Optional<TractStatus> findByTract(TractName name);

    void save(TractStatus status);

    void deleteByTract(TractName name);
}
