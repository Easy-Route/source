package org.dataflow.domain.port.dataplane;

import java.time.Instant;
import java.util.List;

public interface DlqReader {

    List<DlqMessage> peek(String topic, int limit);

    void replay(String topic, int limit);

    record DlqMessage(
            String topic,
            int partition,
            long offset,
            String key,
            String value,
            Instant timestamp,
            String reason
    ) {
    }
}
