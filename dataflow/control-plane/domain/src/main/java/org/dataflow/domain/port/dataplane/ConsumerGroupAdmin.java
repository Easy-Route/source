package org.dataflow.domain.port.dataplane;

import java.util.List;

public interface ConsumerGroupAdmin {

    void register(String groupId, List<String> topics);

    void deregister(String groupId);

    boolean exists(String groupId);

    long lag(String groupId, String topic, int partition);
}
