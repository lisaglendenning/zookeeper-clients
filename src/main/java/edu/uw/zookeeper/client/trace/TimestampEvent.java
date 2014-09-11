package edu.uw.zookeeper.client.trace;

import java.io.IOException;

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

@TraceEventType(TraceEventTag.TIMESTAMP_EVENT)
@JsonSerialize(using=TimestampEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=TimestampEvent.Deserializer.class)
public final class TimestampEvent implements TraceEvent {

    public static TimestampEvent currentTimeMillis() {
        return create(System.currentTimeMillis());
    }

    public static TimestampEvent create(long timestamp) {
        return new TimestampEvent(timestamp);
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
        return MoreObjects.toStringHelper(this)
                .addValue(timestamp)
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

    public static class Serializer extends ListSerializer<TimestampEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(TimestampEvent.class);
        }
    
        @Override
        protected void serializeValue(TimestampEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeNumber(value.timestamp);
        }
    }

    public static class Deserializer extends ListDeserializer<TimestampEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = 3834630520589034004L;
    
        public Deserializer() {
            super(TimestampEvent.class);
        }
    
        @Override
        protected TimestampEvent deserializeValue(JsonParser json,
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
            long timestamp = json.getLongValue();
            json.clearCurrentToken();
            TimestampEvent value = new TimestampEvent(timestamp);
            return value;
        }
    }
}
