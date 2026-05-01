package org.dataflow.flink.sink;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.dataflow.flink.event.CdcEvent;

public class PoisonGuard extends ProcessFunction<CdcEvent, CdcEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(CdcEvent event, Context ctx, Collector<CdcEvent> out) {
        try {
            if (event.source() == null || (event.after() == null && event.before() == null)) {
                ctx.output(DlqRouter.DLQ_TAG, new DlqRouter.DlqMessage(
                        event, "missing-payload", "PoisonGuard", System.currentTimeMillis()));
                return;
            }
            out.collect(event);
        } catch (RuntimeException e) {
            ctx.output(DlqRouter.DLQ_TAG, new DlqRouter.DlqMessage(
                    event, e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "PoisonGuard", System.currentTimeMillis()));
        }
    }
}
