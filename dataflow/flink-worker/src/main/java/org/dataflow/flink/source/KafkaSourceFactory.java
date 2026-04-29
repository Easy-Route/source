package org.dataflow.flink.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.dataflow.flink.config.TractConfig;
import org.dataflow.flink.event.CdcEvent;
import org.dataflow.flink.event.CdcEventDeserializer;

import java.util.List;

public final class KafkaSourceFactory {

    private KafkaSourceFactory() {
    }

    public static KafkaSource<CdcEvent> build(TractConfig cfg, String bootstrapServers, String consumerGroup) {
        List<String> topics = cfg.spec().source().publication().tables().stream()
                .map(t -> cfg.metadata().labels().getOrDefault("source", "src") + "." + t.replace('.', '_'))
                .toList();
        return KafkaSource.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(CdcEventDeserializer.valueSchema())
                .setProperty("isolation.level", "read_committed")
                .setProperty("auto.offset.reset", "earliest")
                .build();
    }
}
