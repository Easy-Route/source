package org.dataflow.domain.status;

public enum ComponentKind {
    DEBEZIUM_CONNECTOR,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    FLINK_JOB,
    SINK_CONNECTOR
}
