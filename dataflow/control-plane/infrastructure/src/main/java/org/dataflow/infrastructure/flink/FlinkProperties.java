package org.dataflow.infrastructure.flink;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dataflow.flink")
public record FlinkProperties(
        String url,
        String workerJarPath,
        String workerEntryClass,
        String checkpointStorage,
        String savepointStorage,
        int checkpointIntervalSeconds,
        int defaultParallelism,
        int taskSlots
) {
    public FlinkProperties {
        if (url == null || url.isBlank()) {
            url = "http://flink-jobmanager:8081";
        }
        if (workerJarPath == null || workerJarPath.isBlank()) {
            workerJarPath = "/opt/flink/usrlib/flink-worker.jar";
        }
        if (workerEntryClass == null || workerEntryClass.isBlank()) {
            workerEntryClass = "org.dataflow.flink.FlinkWorkerJob";
        }
        if (checkpointStorage == null || checkpointStorage.isBlank()) {
            checkpointStorage = "file:///flink-checkpoints";
        }
        if (savepointStorage == null || savepointStorage.isBlank()) {
            savepointStorage = checkpointStorage;
        }
        if (checkpointIntervalSeconds <= 0) {
            checkpointIntervalSeconds = 30;
        }
        if (defaultParallelism <= 0) {
            defaultParallelism = 2;
        }
        if (taskSlots <= 0) {
            taskSlots = 4;
        }
    }
}
