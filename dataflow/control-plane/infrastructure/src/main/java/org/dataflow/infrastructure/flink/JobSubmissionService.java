package org.dataflow.infrastructure.flink;

import org.dataflow.domain.port.dataplane.FlinkJobClient;
import org.dataflow.domain.port.dataplane.FlinkJobClient.JobSubmission;
import org.dataflow.domain.tract.Tract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

@Service
public class JobSubmissionService {

    private static final Logger log = LoggerFactory.getLogger(JobSubmissionService.class);

    private final FlinkJobClient flink;
    private final FlinkProperties props;
    private final TractConfigPublisher configPublisher;

    public JobSubmissionService(FlinkJobClient flink,
                                FlinkProperties props,
                                TractConfigPublisher configPublisher) {
        this.flink = flink;
        this.props = props;
        this.configPublisher = configPublisher;
    }

    public String ensureJarUploaded() {
        String jarFilename = jarFilename();
        if (flink.jarExists(jarFilename)) {
            return jarFilename;
        }
        try {
            byte[] bytes = Files.readAllBytes(Path.of(props.workerJarPath()));
            return flink.uploadJar(bytes, jarFilename);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "flink-worker jar not found at " + props.workerJarPath(), e);
        }
    }

    public String submit(Tract tract, Optional<String> savepointPath) {
        String jarId = ensureJarUploaded();
        String configPath = configPublisher.publish(tract);
        JobSubmission submission = new JobSubmission(
                props.workerEntryClass(),
                "--config " + configPath
                        + " --tract " + tract.name().value()
                        + " --consumer-group " + consumerGroupFor(tract),
                props.defaultParallelism(),
                Map.of()
        );
        String jobId = savepointPath
                .map(sp -> flink.submitJobFromSavepoint(jarId, submission, sp))
                .orElseGet(() -> flink.submitJob(jarId, submission));
        log.info("Tract {} submitted as Flink job {}", tract.name(), jobId);
        return jobId;
    }

    public String savepointAndCancel(String jobId) {
        String requestId = flink.stopWithSavepoint(jobId, props.savepointStorage());
        log.info("Stop-with-savepoint requested for {} (request {})", jobId, requestId);
        return requestId;
    }

    public void cancel(String jobId) {
        flink.cancelJob(jobId);
    }

    public static String consumerGroupFor(Tract tract) {
        return "dataflow-tract-" + tract.name().value();
    }

    private String jarFilename() {
        Path p = Path.of(props.workerJarPath());
        return p.getFileName().toString();
    }
}
