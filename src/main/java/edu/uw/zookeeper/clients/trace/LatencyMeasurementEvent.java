package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.base.Objects;

@TraceEventType(TraceEventTag.LATENCY_MEASUREMENT_EVENT)
public final class LatencyMeasurementEvent implements TraceEvent {

    public static LatencyMeasurementEvent from(LatencyEvent event) {
        return new LatencyMeasurementEvent((int) TimeUnit.MICROSECONDS.convert(event.getNanos(), TimeUnit.NANOSECONDS));
    }
    
    @JacksonDeserializer
    public static LatencyMeasurementEvent deserialize(JsonParser parser) throws JsonParseException, IOException {
        int micros = parser.getIntValue();
        return new LatencyMeasurementEvent(micros);
    }

    @JacksonSerializer
    public static void serialize(LatencyMeasurementEvent value, JsonGenerator generator) throws JsonGenerationException, IOException {
        generator.writeNumber(value.micros);
    }
    
    private final int micros;
    
    @JsonCreator
    public LatencyMeasurementEvent(@JsonProperty("micros") int micros) {
        this.micros = micros;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.LATENCY_MEASUREMENT_EVENT;
    }
    
    public int getMicros() {
        return micros;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("micros", micros)
                .toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof LatencyMeasurementEvent)) {
            return false;
        }
        return micros == ((LatencyMeasurementEvent) obj).micros;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(micros);
    }
}
