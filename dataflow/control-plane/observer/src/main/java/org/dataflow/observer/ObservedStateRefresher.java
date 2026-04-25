package org.dataflow.observer;

import org.dataflow.domain.port.repository.TractRepository;
import org.dataflow.domain.port.repository.TractStatusRepository;
import org.dataflow.domain.tract.ReconciliationStatus;
import org.dataflow.domain.tract.Tract;
import org.dataflow.domain.tract.TractStatus;
import org.dataflow.observer.probe.FlinkJobProbe;
import org.dataflow.observer.probe.KafkaConnectorProbe;
import org.dataflow.observer.probe.KafkaTopicProbe;
import org.dataflow.observer.probe.SinkConnectorProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

@Component
@EnableConfigurationProperties(ObserverProperties.class)
public class ObservedStateRefresher {

    private static final Logger log = LoggerFactory.getLogger(ObservedStateRefresher.class);

    private final TractRepository tracts;
    private final TractStatusRepository statuses;
    private final KafkaConnectorProbe connectorProbe;
    private final KafkaTopicProbe topicProbe;
    private final FlinkJobProbe flinkProbe;
    private final SinkConnectorProbe sinkProbe;
    private final MetricsExporter metrics;

    public ObservedStateRefresher(TractRepository tracts,
                                  TractStatusRepository statuses,
                                  KafkaConnectorProbe connectorProbe,
                                  KafkaTopicProbe topicProbe,
                                  FlinkJobProbe flinkProbe,
                                  SinkConnectorProbe sinkProbe,
                                  MetricsExporter metrics) {
        this.tracts = tracts;
        this.statuses = statuses;
        this.connectorProbe = connectorProbe;
        this.topicProbe = topicProbe;
        this.flinkProbe = flinkProbe;
        this.sinkProbe = sinkProbe;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${dataflow.observer.flink-probe-interval:PT10S}")
    public void refresh() {
        for (Tract tract : tracts.findAll()) {
            try {
                refreshOne(tract);
            } catch (RuntimeException e) {
                log.warn("Probe cycle failed for {}: {}", tract.name(), e.toString());
            }
        }
    }

    private void refreshOne(Tract tract) {
        var connector = connectorProbe.probe(tract.source());
        var sourceTopics = topicProbe.probeSourceTopics(
                tract.source(), tract.spec().source().publication().tables());
        var dlq = topicProbe.probeDlq(tract.name());
        Optional<TractStatus> existing = statuses.findByTract(tract.name());
        Optional<String> jobId = existing.flatMap(s -> s.flinkJobId());
        var flinkJob = jobId.flatMap(flinkProbe::probe);
        var sinks = sinkProbe.probe(tract.spec().sinks());

        ReconciliationStatus recon = computeStatus(connector.isPresent(), flinkJob.isPresent());
        TractStatus status = new TractStatus(
                tract.name(),
                tract.desiredState(),
                recon,
                connector,
                java.util.stream.Stream.concat(sourceTopics.stream(), dlq.stream()).toList(),
                flinkJob,
                sinks,
                jobId,
                existing.flatMap(s -> s.lastError()),
                tract.specVersion().value(),
                Instant.now()
        );
        statuses.save(status);
        metrics.recordReconciliationStatus(tract.name(), recon.name());
    }

    private ReconciliationStatus computeStatus(boolean connectorPresent, boolean flinkPresent) {
        if (connectorPresent && flinkPresent) {
            return ReconciliationStatus.RECONCILED;
        }
        return ReconciliationStatus.DRIFT;
    }
}
