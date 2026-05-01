package org.dataflow.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.util.OutputTag;
import org.dataflow.flink.event.CdcEvent;

import java.util.Properties;

public final class DlqRouter {

    public static final OutputTag<DlqMessage> DLQ_TAG = new OutputTag<>("dlq-messages") {
    };

    private DlqRouter() {
    }

    public static void route(SideOutputDataStream<DlqMessage> dlqStream,
                             String dlqTopic,
                             String bootstrap) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrap);
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(dlqTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        dlqStream.map(DlqMessage::serialize).sinkTo(sink).name("kafka-dlq-sink");
    }

    public record DlqMessage(
            CdcEvent event,
            String reason,
            String stage,
            long detectedAtMs
    ) {
        public String serialize() {
            return "{"
                    + "\"reason\":\"" + escape(reason) + "\","
                    + "\"stage\":\"" + escape(stage) + "\","
                    + "\"detectedAtMs\":" + detectedAtMs + ","
                    + "\"event\":{"
                    + "\"op\":\"" + escape(event.op()) + "\","
                    + "\"table\":\"" + escape(event.tableFqn()) + "\","
                    + "\"dedupKey\":\"" + escape(event.dedupKey()) + "\""
                    + "}}";
        }

        private static String escape(String s) {
            return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
