package edu.uw.zookeeper.client.trace;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

@TraceEventType(TraceEventTag.LATENCY_MEASUREMENT_EVENT)
@JsonSerialize(using=LatencyMeasurementEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=LatencyMeasurementEvent.Deserializer.class)
public final class LatencyMeasurementEvent implements TraceEvent {

    public static LatencyMeasurementEvent fromNanos(long nanos) {
        long micros = (nanos <= 0) ? 
                nanos : 
                    TimeUnit.MICROSECONDS.convert(nanos, TimeUnit.NANOSECONDS);
        return new LatencyMeasurementEvent((int) micros);
    }

    public static LatencyMeasurementEvent create(int micros) {
        return new LatencyMeasurementEvent(micros);
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
        return MoreObjects.toStringHelper(this)
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

    public static class Serializer extends ListSerializer<LatencyMeasurementEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(LatencyMeasurementEvent.class);
        }
    
        @Override
        protected void serializeValue(LatencyMeasurementEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeNumber(value.micros);
        }
    }

    public static class Deserializer extends ListDeserializer<LatencyMeasurementEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = -6386686891863027790L;
    
        public Deserializer() {
            super(LatencyMeasurementEvent.class);
        }
    
        @Override
        protected LatencyMeasurementEvent deserializeValue(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            JsonToken token = json.getCurrentToken();
            if (token == null) {
                token = json.nextToken();
                if (token == null) {
                    return null;
                }
            }
            if (token != JsonToken.VALUE_NUMBER_INT) {
                throw ctxt.wrongTokenException(json, JsonToken.VALUE_NUMBER_INT, "");
            }
            int micros = json.getIntValue();
            json.clearCurrentToken();
            LatencyMeasurementEvent value = new LatencyMeasurementEvent(micros);
            return value;
        }
    }
}
