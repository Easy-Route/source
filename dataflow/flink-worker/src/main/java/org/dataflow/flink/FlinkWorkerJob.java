package org.dataflow.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.dataflow.flink.config.ConfigLoader;
import org.dataflow.flink.config.TractConfig;
import org.dataflow.flink.event.CdcEvent;
import org.dataflow.flink.source.KafkaSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class FlinkWorkerJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkWorkerJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        Path configPath = Path.of(params.getRequired("config"));
        String tractName = params.getRequired("tract");
        String consumerGroup = params.getRequired("consumer-group");
        String bootstrap = params.get("bootstrap", System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092"));

        TractConfig cfg = ConfigLoader.load(configPath);
        log.info("Starting flink-worker for tract={} (config={})", tractName, configPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(params.getInt("parallelism", 2));

        EmbeddedRocksDBStateBackend rocks = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocks);
        env.getCheckpointConfig().setCheckpointStorage(
                params.get("checkpoint-storage", "file:///flink-checkpoints"));
        env.enableCheckpointing(params.getLong("checkpoint-interval-ms", 30_000L));
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        var source = KafkaSourceFactory.build(cfg, bootstrap, consumerGroup);
        var stream = env.fromSource(source, WatermarkStrategy.<CdcEvent>noWatermarks(),
                "kafka-source-" + tractName);

        // Operators: Dedup -> Filter -> Timezone -> Sink land in subsequent commits.
        stream.print("raw-" + tractName);

        env.execute("dataflow-tract-" + tractName);
    }
}
