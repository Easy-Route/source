package org.dataflow.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.dataflow.flink.config.ConfigLoader;
import org.dataflow.flink.config.TractConfig;
import org.dataflow.flink.event.CdcEvent;
import org.dataflow.flink.operators.DeduplicationFunction;
import org.dataflow.flink.operators.OperationFilter;
import org.dataflow.flink.operators.TimezoneTransform;
import org.dataflow.flink.source.KafkaSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

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
        DataStream<CdcEvent> raw = env.fromSource(source, WatermarkStrategy.<CdcEvent>noWatermarks(),
                "kafka-source-" + tractName);

        long ttlHours = cfg.spec().transformation() == null
                || cfg.spec().transformation().deduplication() == null
                || cfg.spec().transformation().deduplication().ttlHours() == null
                ? 24L
                : cfg.spec().transformation().deduplication().ttlHours();

        DataStream<CdcEvent> deduped = raw
                .keyBy(CdcEvent::dedupKey)
                .process(new DeduplicationFunction(ttlHours))
                .name("dedup-" + tractName);

        List<String> excluded = cfg.spec().transformation() == null
                || cfg.spec().transformation().filter() == null
                ? List.of()
                : cfg.spec().transformation().filter().excludeOperations();
        DataStream<CdcEvent> filtered = deduped.filter(new OperationFilter(excluded))
                .name("filter-" + tractName);

        String fromTz = cfg.spec().transformation() == null
                || cfg.spec().transformation().timezone() == null
                ? "UTC" : cfg.spec().transformation().timezone().from();
        String toTz = cfg.spec().transformation() == null
                || cfg.spec().transformation().timezone() == null
                ? "UTC" : cfg.spec().transformation().timezone().to();
        DataStream<CdcEvent> normalized = filtered.map(new TimezoneTransform(fromTz, toTz))
                .name("tz-" + tractName);

        // Sinks land in the next commit.
        normalized.print("normalized-" + tractName);

        env.execute("dataflow-tract-" + tractName);
    }
}
