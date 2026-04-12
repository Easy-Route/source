package org.dataflow.domain.port.dataplane;

import org.dataflow.domain.status.TopicStatus;

import java.util.List;
import java.util.Optional;

public interface KafkaTopicAdmin {

    void createTopic(TopicSpec spec);

    void deleteTopic(String topicName);

    boolean topicExists(String topicName);

    Optional<TopicStatus> describeTopic(String topicName);

    List<TopicStatus> describeTopics(List<String> topicNames);

    void registerConsumerGroup(String groupId, List<String> topics);

    void deleteConsumerGroup(String groupId);

    record TopicSpec(
            String name,
            int partitions,
            short replicationFactor,
            long retentionMs,
            String cleanupPolicy
    ) {
    }
}
