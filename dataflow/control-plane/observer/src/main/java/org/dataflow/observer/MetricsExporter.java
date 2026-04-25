package org.dataflow.observer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.dataflow.domain.tract.TractName;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

@Component
public class MetricsExporter {

    private final MeterRegistry registry;
    private final Map<TractName, AtomicLong> lagGauges = new ConcurrentHashMap<>();
    private final Map<TractName, AtomicLong> dlqCounters = new ConcurrentHashMap<>();
    private final Map<TractName, Counter> recordsInCounters = new ConcurrentHashMap<>();
    private final Map<TractName, Timer> latencyTimers = new ConcurrentHashMap<>();

    public MetricsExporter(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordEndToEndLatency(TractName tract, long millis) {
        latencyTimers.computeIfAbsent(tract, t -> Timer.builder("dataflow.tract.latency.e2e")
                .tags(Tags.of("tract", t.value()))
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
        ).record(java.time.Duration.ofMillis(millis));
    }

    public void incrementRecordsIn(TractName tract, long delta) {
        recordsInCounters.computeIfAbsent(tract, t -> Counter.builder("dataflow.tract.records.in")
                .tags(Tags.of("tract", t.value()))
                .register(registry)
        ).increment(delta);
    }

    public void recordSourceLagSeconds(TractName tract, long seconds) {
        AtomicLong holder = lagGauges.computeIfAbsent(tract, t -> {
            AtomicLong lag = new AtomicLong(0);
            Gauge.builder("dataflow.tract.source.lag.seconds", lag, (ToDoubleFunction<AtomicLong>) AtomicLong::get)
                    .tags(Tags.of("tract", t.value()))
                    .register(registry);
            return lag;
        });
        holder.set(seconds);
    }

    public void incrementDlq(TractName tract, long delta) {
        AtomicLong holder = dlqCounters.computeIfAbsent(tract, t -> {
            AtomicLong dlq = new AtomicLong(0);
            Gauge.builder("dataflow.tract.dlq.size", dlq, (ToDoubleFunction<AtomicLong>) AtomicLong::get)
                    .tags(Tags.of("tract", t.value()))
                    .register(registry);
            return dlq;
        });
        holder.addAndGet(delta);
    }

    public void recordReconciliationStatus(TractName tract, String status) {
        registry.counter("dataflow.tract.reconciliation.transitions",
                "tract", tract.value(),
                "status", status
        ).increment();
    }
}
