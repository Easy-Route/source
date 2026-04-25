package org.dataflow.observer.probe;

import org.dataflow.domain.port.dataplane.KafkaTopicAdmin;
import org.dataflow.domain.source.SourceName;
import org.dataflow.domain.status.TopicStatus;
import org.dataflow.domain.tract.TractName;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaTopicProbe {

    private final KafkaTopicAdmin topics;

    public KafkaTopicProbe(KafkaTopicAdmin topics) {
        this.topics = topics;
    }

    public List<TopicStatus> probeSourceTopics(SourceName source, List<String> tableFqns) {
        List<String> topicNames = tableFqns.stream()
                .map(t -> source.value() + "." + t.replace('.', '_'))
                .toList();
        return topics.describeTopics(topicNames);
    }

    public List<TopicStatus> probeDlq(TractName tract) {
        return topics.describeTopics(List.of("dlq." + tract.value()));
    }
}
