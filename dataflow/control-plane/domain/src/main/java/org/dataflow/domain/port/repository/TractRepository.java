package org.dataflow.domain.port.repository;

import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractName;

import java.util.List;
import java.util.Optional;

public interface TractRepository {

    Optional<Tract> findByName(TractName name);

    List<Tract> findAll();

    List<Tract> findBySource(SourceName source);

    List<Tract> findRequiringReconciliation();

    void save(Tract tract);

    void delete(TractName name);

    boolean existsByName(TractName name);
}
