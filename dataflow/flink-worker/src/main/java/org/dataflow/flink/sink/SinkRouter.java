package org.dataflow.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.dataflow.flink.config.TractConfig;
import org.dataflow.flink.event.CdcEvent;

public final class SinkRouter {

    private SinkRouter() {
    }

    public static void wireSinks(DataStream<CdcEvent> stream, TractConfig cfg) {
        for (TractConfig.Sink sink : cfg.spec().sinks()) {
            for (String tableFqn : cfg.spec().source().publication().tables()) {
                String mapped = mappedTable(sink, tableFqn);
                DataStream<CdcEvent> routed = stream
                        .filter(e -> e.tableFqn().equals(tableFqn))
                        .name("route-" + sink.name() + "-" + mapped);
                StarRocksSinkFactory.attach(routed, sink, mapped);
            }
        }
    }

    private static String mappedTable(TractConfig.Sink sink, String sourceFqn) {
        if (sink.mapping() != null && sink.mapping().tableOverrides() != null) {
            String override = sink.mapping().tableOverrides().get(sourceFqn);
            if (override != null) {
                return override;
            }
        }
        int dot = sourceFqn.indexOf('.');
        return dot >= 0 ? sourceFqn.substring(dot + 1) : sourceFqn;
    }
}
