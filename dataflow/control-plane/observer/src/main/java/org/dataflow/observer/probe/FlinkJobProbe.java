package org.dataflow.observer.probe;

import org.dataflow.domain.port.dataplane.FlinkJobClient;
import org.dataflow.domain.port.dataplane.FlinkJobClient.JobMetric;
import org.dataflow.domain.status.ComponentStatus;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

@Component
public class FlinkJobProbe {

    private static final List<String> METRICS_OF_INTEREST = List.of(
            "numRecordsIn", "numRecordsOut", "currentInputWatermark", "isBackPressured");

    private final FlinkJobClient flink;

    public FlinkJobProbe(FlinkJobClient flink) {
        this.flink = flink;
    }

    public Optional<ComponentStatus> probe(String jobId) {
        return flink.jobStatus(jobId);
    }

    public List<JobMetric> metrics(String jobId) {
        return flink.jobMetrics(jobId, METRICS_OF_INTEREST);
    }
}
