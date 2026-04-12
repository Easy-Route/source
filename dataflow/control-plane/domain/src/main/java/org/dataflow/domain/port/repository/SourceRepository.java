package org.dataflow.domain.port.repository;

import org.dataflow.domain.source.Source;
import org.dataflow.domain.source.SourceName;

import java.util.List;
import java.util.Optional;

public interface SourceRepository {

    Optional<Source> findByName(SourceName name);

    List<Source> findAll();

    void save(Source source);

    void delete(SourceName name);

    boolean existsByName(SourceName name);

    long countTractsReferencing(SourceName name);
}
