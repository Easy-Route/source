package org.dataflow.flink.operators;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.dataflow.flink.event.CdcEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class DeduplicationFunction extends KeyedProcessFunction<String, CdcEvent, CdcEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(DeduplicationFunction.class);

    private final long ttlHours;

    private transient ValueState<Boolean> seen;

    public DeduplicationFunction(long ttlHours) {
        this.ttlHours = ttlHours;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(ttlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(1000, false)
                .build();
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                "dedup-seen", TypeInformation.of(Boolean.class));
        descriptor.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(CdcEvent event, Context ctx, Collector<CdcEvent> out) throws Exception {
        Boolean already = seen.value();
        if (already != null && already) {
            log.debug("Duplicate CDC event dropped: {}", event.dedupKey());
            return;
        }
        seen.update(Boolean.TRUE);
        out.collect(event);
    }

    public static Duration defaultTtl() {
        return Duration.ofHours(24);
    }
}
