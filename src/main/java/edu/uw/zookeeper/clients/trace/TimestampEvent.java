package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.Type;

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

@TraceEventType(TraceEventTag.TIMESTAMP_EVENT)
@JsonSerialize(using=TimestampEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=TimestampEvent.Deserializer.class)
public final class TimestampEvent implements TraceEvent {

    public static TimestampEvent currentTimeMillis() {
        return new TimestampEvent(System.currentTimeMillis());
    }
    
    private final long timestamp;

    @JsonCreator
    public TimestampEvent(@JsonProperty("timestamp") long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.TIMESTAMP_EVENT;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("timestamp", timestamp)
                .toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof TimestampEvent)) {
            return false;
        }
        return timestamp == ((TimestampEvent) obj).timestamp;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(timestamp);
    }

    public static class Serializer extends StdSerializer<TimestampEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(TimestampEvent.class);
        }
    
        @Override
        public void serialize(TimestampEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeStartArray();
            json.writeNumber(value.timestamp);
            json.writeEndArray();
        }
    
        @Override
        public JsonNode getSchema(SerializerProvider provider, Type typeHint)
            throws JsonMappingException {
            return createSchemaNode("array");
        }
    }

    public static class Deserializer extends StdDeserializer<TimestampEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = 3834630520589034004L;
    
        public Deserializer() {
            super(TimestampEvent.class);
        }
    
        @Override
        public TimestampEvent deserialize(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            if (! json.isExpectedStartArrayToken()) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.START_ARRAY.toString());
            }
            json.nextToken();
            long timestamp = json.getLongValue();
            json.nextToken();
            if (json.getCurrentToken() != JsonToken.END_ARRAY) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.END_ARRAY.toString());
            }
            json.nextToken();
            TimestampEvent value = new TimestampEvent(timestamp);
            return value;
        }
        
        @Override
        public boolean isCachable() { 
            return true; 
        }
    }
}
