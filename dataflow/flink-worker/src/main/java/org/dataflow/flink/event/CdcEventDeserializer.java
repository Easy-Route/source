package org.dataflow.flink.event;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class CdcEventDeserializer implements KafkaRecordDeserializationSchema<CdcEvent> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CdcEvent> out) throws IOException {
        if (record.value() == null) {
            // Tombstones (Debezium delete markers) are dropped here; the
            // preceding 'd' event already carries the delete intent.
            return;
        }
        CdcEvent event = MAPPER.readValue(record.value(), CdcEvent.class);
        out.collect(event);
    }

    @Override
    public TypeInformation<CdcEvent> getProducedType() {
        return TypeInformation.of(CdcEvent.class);
    }

    public static DeserializationSchema<CdcEvent> valueSchema() {
        return new DeserializationSchema<>() {
            @Override
            public CdcEvent deserialize(byte[] bytes) throws IOException {
                return MAPPER.readValue(bytes, CdcEvent.class);
            }

            @Override
            public boolean isEndOfStream(CdcEvent next) {
                return false;
            }

            @Override
            public TypeInformation<CdcEvent> getProducedType() {
                return TypeInformation.of(CdcEvent.class);
            }
        };
    }
}
