package org.dataflow.infrastructure.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.dataflow.domain.port.dataplane.ConsumerGroupAdmin;
import org.dataflow.domain.port.dataplane.KafkaTopicAdmin;
import org.dataflow.domain.status.ComponentHealth;
import org.dataflow.domain.status.TopicStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaAdminClientAdapter implements KafkaTopicAdmin, ConsumerGroupAdmin, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClientAdapter.class);

    private final AdminClient admin;
    private final KafkaProperties props;

    public KafkaAdminClientAdapter(AdminClient admin, KafkaProperties props) {
        this.admin = admin;
        this.props = props;
    }

    @Override
    public void createTopic(TopicSpec spec) {
        NewTopic topic = new NewTopic(spec.name(), spec.partitions(), spec.replicationFactor());
        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", Long.toString(spec.retentionMs()));
        if (spec.cleanupPolicy() != null) {
            configs.put("cleanup.policy", spec.cleanupPolicy());
        }
        topic.configs(configs);
        try {
            admin.createTopics(List.of(topic)).all().get();
            log.info("Topic created: {} (partitions={}, rf={})",
                    spec.name(), spec.partitions(), spec.replicationFactor());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted creating topic " + spec.name(), e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                log.debug("Topic {} already exists", spec.name());
                return;
            }
            throw new KafkaAdminException("Failed to create topic " + spec.name(), e);
        }
    }

    @Override
    public void deleteTopic(String topicName) {
        try {
            admin.deleteTopics(List.of(topicName)).all().get();
            log.info("Topic deleted: {}", topicName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted deleting topic " + topicName, e);
        } catch (ExecutionException e) {
            throw new KafkaAdminException("Failed to delete topic " + topicName, e);
        }
    }

    @Override
    public boolean topicExists(String topicName) {
        try {
            return admin.listTopics().names().get().contains(topicName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            throw new KafkaAdminException("Failed to list topics", e);
        }
    }

    @Override
    public Optional<TopicStatus> describeTopic(String topicName) {
        try {
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(List.of(topicName)).allTopicNames().get();
            TopicDescription desc = descriptions.get(topicName);
            if (desc == null) {
                return Optional.empty();
            }
            ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Config cfg = admin.describeConfigs(List.of(res)).all().get().get(res);
            long retention = parseRetentionMs(cfg);
            int partitions = desc.partitions().size();
            short rf = (short) desc.partitions().get(0).replicas().size();
            return Optional.of(new TopicStatus(topicName, partitions, rf, retention,
                    ComponentHealth.RUNNING, Instant.now()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            log.warn("describeTopic({}) failed: {}", topicName, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public List<TopicStatus> describeTopics(List<String> topicNames) {
        return topicNames.stream()
                .map(this::describeTopic)
                .flatMap(Optional::stream)
                .toList();
    }

    @Override
    public void registerConsumerGroup(String groupId, List<String> topics) {
        // consumer groups in Kafka are created lazily on first consume; the
        // control plane does not pre-create them, but it does verify the
        // requested groupId is not already in use by a different tract
        // (handled via metadata-store invariants).
        log.debug("Reserving consumer group {} for topics {}", groupId, topics);
    }

    @Override
    public void deleteConsumerGroup(String groupId) {
        try {
            admin.deleteConsumerGroups(List.of(groupId)).all().get();
            log.info("Consumer group deleted: {}", groupId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.warn("Failed to delete consumer group {}: {}", groupId, e.getMessage());
        }
    }

    @Override
    public void register(String groupId, List<String> topics) {
        registerConsumerGroup(groupId, topics);
    }

    @Override
    public void deregister(String groupId) {
        deleteConsumerGroup(groupId);
    }

    @Override
    public boolean exists(String groupId) {
        try {
            return admin.listConsumerGroups().all().get().stream()
                    .anyMatch(g -> g.groupId().equals(groupId));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            return false;
        }
    }

    @Override
    public long lag(String groupId, String topic, int partition) {
        // Real implementation joins listConsumerGroupOffsets with
        // listOffsets(LATEST). Stubbed here for brevity.
        return -1L;
    }

    @Override
    public void destroy() {
        admin.close(Duration.ofSeconds(5));
    }

    private long parseRetentionMs(Config cfg) {
        ConfigEntry entry = cfg.get("retention.ms");
        if (entry == null || entry.value() == null) {
            return props.defaultRetentionMs();
        }
        try {
            return Long.parseLong(entry.value());
        } catch (NumberFormatException e) {
            return props.defaultRetentionMs();
        }
    }
}
