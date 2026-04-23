package org.dataflow.reconciler.plan;

import org.dataflow.domain.port.dataplane.ConsumerGroupAdmin;
import org.dataflow.domain.port.dataplane.FlinkJobClient;
import org.dataflow.domain.port.dataplane.KafkaConnectClient;
import org.dataflow.domain.port.dataplane.KafkaTopicAdmin;
import org.dataflow.domain.port.dataplane.StarRocksClient;
import org.dataflow.domain.port.repository.SourceRepository;
import org.dataflow.domain.port.repository.TractStatusRepository;
import org.dataflow.domain.tract.Tract;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class StepContext {

    private final Tract tract;
    private final KafkaTopicAdmin topics;
    private final ConsumerGroupAdmin groups;
    private final KafkaConnectClient connect;
    private final FlinkJobClient flink;
    private final StarRocksClient starRocks;
    private final SourceRepository sources;
    private final TractStatusRepository statuses;
    private final Map<String, Object> scratch = new HashMap<>();

    public StepContext(Tract tract,
                       KafkaTopicAdmin topics,
                       ConsumerGroupAdmin groups,
                       KafkaConnectClient connect,
                       FlinkJobClient flink,
                       StarRocksClient starRocks,
                       SourceRepository sources,
                       TractStatusRepository statuses) {
        this.tract = tract;
        this.topics = topics;
        this.groups = groups;
        this.connect = connect;
        this.flink = flink;
        this.starRocks = starRocks;
        this.sources = sources;
        this.statuses = statuses;
    }

    public Tract tract() {
        return tract;
    }

    public KafkaTopicAdmin topics() {
        return topics;
    }

    public ConsumerGroupAdmin groups() {
        return groups;
    }

    public KafkaConnectClient connect() {
        return connect;
    }

    public FlinkJobClient flink() {
        return flink;
    }

    public StarRocksClient starRocks() {
        return starRocks;
    }

    public SourceRepository sources() {
        return sources;
    }

    public TractStatusRepository statuses() {
        return statuses;
    }

    public void store(String key, Object value) {
        scratch.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> read(String key) {
        return Optional.ofNullable((T) scratch.get(key));
    }
}
