package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Objects;

@TraceEventType(TraceEventTag.LATENCY_MEASUREMENT_EVENT)
@JsonSerialize(using=LatencyMeasurementEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=LatencyMeasurementEvent.Deserializer.class)
public final class LatencyMeasurementEvent implements TraceEvent {

    public static LatencyMeasurementEvent from(LatencyEvent event) {
        return new LatencyMeasurementEvent((int) TimeUnit.MICROSECONDS.convert(event.getNanos(), TimeUnit.NANOSECONDS));
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

    public static class Serializer extends StdSerializer<LatencyMeasurementEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(LatencyMeasurementEvent.class);
        }
    
        @Override
        public void serialize(LatencyMeasurementEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeStartArray();
            json.writeNumber(value.micros);
            json.writeEndArray();
        }
    
        @Override
        public JsonNode getSchema(SerializerProvider provider, Type typeHint)
            throws JsonMappingException {
            return createSchemaNode("array");
        }
    }

    public static class Deserializer extends StdDeserializer<LatencyMeasurementEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = -6386686891863027790L;
    
        public Deserializer() {
            super(LatencyMeasurementEvent.class);
        }
    
        @Override
        public LatencyMeasurementEvent deserialize(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            if (! json.isExpectedStartArrayToken()) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.START_ARRAY.toString());
            }
            json.nextToken();
            int micros = json.getIntValue();
            json.nextToken();
            if (json.getCurrentToken() != JsonToken.END_ARRAY) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.END_ARRAY.toString());
            }
            json.nextToken();
            LatencyMeasurementEvent value = new LatencyMeasurementEvent(micros);
            return value;
        }
        
        @Override
        public boolean isCachable() { 
            return true; 
        }
    }
}
