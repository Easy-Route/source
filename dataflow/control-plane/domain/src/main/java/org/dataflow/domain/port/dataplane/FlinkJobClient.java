package org.dataflow.domain.port.dataplane;

import org.dataflow.domain.status.ComponentStatus;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface FlinkJobClient {

    String uploadJar(byte[] jarBytes, String filename);

    boolean jarExists(String jarFilename);

    String submitJob(String jarId, JobSubmission submission);

    void cancelJob(String jobId);

    String triggerSavepoint(String jobId, String savepointDir);

    String stopWithSavepoint(String jobId, String savepointDir);

    String submitJobFromSavepoint(String jarId, JobSubmission submission, String savepointPath);

    Optional<ComponentStatus> jobStatus(String jobId);

    List<JobMetric> jobMetrics(String jobId, List<String> metricNames);

    record JobSubmission(
            String entryClass,
            String programArgs,
            int parallelism,
            Map<String, String> jobConfig
    ) {
    }

    record JobMetric(String id, String value) {
    }
}
