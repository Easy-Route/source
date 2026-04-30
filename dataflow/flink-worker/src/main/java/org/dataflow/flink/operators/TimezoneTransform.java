package org.dataflow.flink.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.dataflow.flink.event.CdcEvent;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

public class TimezoneTransform implements MapFunction<CdcEvent, CdcEvent> {

    private static final long serialVersionUID = 1L;

    private final ZoneId fromZone;
    private final ZoneId toZone;

    public TimezoneTransform(String fromZoneId, String toZoneId) {
        this.fromZone = ZoneId.of(fromZoneId == null ? "UTC" : fromZoneId);
        this.toZone = ZoneId.of(toZoneId == null ? "UTC" : toZoneId);
    }

    @Override
    public CdcEvent map(CdcEvent event) {
        if (fromZone.equals(toZone)) {
            return event;
        }
        return new CdcEvent(
                event.op(),
                shiftMap(event.before()),
                shiftMap(event.after()),
                event.source(),
                event.tsMs()
        );
    }

    private Map<String, Object> shiftMap(Map<String, Object> input) {
        if (input == null) {
            return null;
        }
        Map<String, Object> out = new HashMap<>(input.size());
        for (Map.Entry<String, Object> e : input.entrySet()) {
            out.put(e.getKey(), shiftValue(e.getValue()));
        }
        return out;
    }

    private Object shiftValue(Object value) {
        if (!(value instanceof String s)) {
            return value;
        }
        return shiftIso(s);
    }

    private Object shiftIso(String s) {
        try {
            OffsetDateTime odt = OffsetDateTime.parse(s);
            return odt.atZoneSameInstant(toZone).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException ignored) {
        }
        try {
            LocalDateTime ldt = LocalDateTime.parse(s);
            ZonedDateTime zdt = ldt.atZone(fromZone).withZoneSameInstant(toZone);
            return zdt.toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (DateTimeParseException ignored) {
        }
        return s;
    }
}
