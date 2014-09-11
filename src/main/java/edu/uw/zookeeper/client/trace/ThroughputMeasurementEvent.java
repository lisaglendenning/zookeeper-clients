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

@TraceEventType(TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT)
@JsonSerialize(using=ThroughputMeasurementEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=ThroughputMeasurementEvent.Deserializer.class)
public final class ThroughputMeasurementEvent implements TraceEvent {

    public static ThroughputMeasurementEvent fromNanos(int count, long nanos) {
        long millis = (nanos <= 0) ? 
                nanos : 
                    TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
        return new ThroughputMeasurementEvent(count, (int) millis);
    }

    public static ThroughputMeasurementEvent create(int count, int millis) {
        return new ThroughputMeasurementEvent(count, millis);
    }

    private final int count;
    private final int millis;
    
    @JsonCreator
    public ThroughputMeasurementEvent(
            @JsonProperty("count") int count,
            @JsonProperty("millis") int millis) {
        this.count = count;
        this.millis = millis;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT;
    }

    public int getCount() {
        return count;
    }

    public int getMillis() {
        return millis;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("count", count)
                .add("millis", millis)
                .toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof ThroughputMeasurementEvent)) {
            return false;
        }
        ThroughputMeasurementEvent other = (ThroughputMeasurementEvent) obj;
        return (count == other.count) && (millis == other.millis);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(count, millis);
    }

    public static class Serializer extends ListSerializer<ThroughputMeasurementEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(ThroughputMeasurementEvent.class);
        }
    
        @Override
        protected void serializeValue(ThroughputMeasurementEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeNumber(value.count);
            json.writeNumber(value.millis);
        }
    }

    public static class Deserializer extends ListDeserializer<ThroughputMeasurementEvent> {

        public static Deserializer create() {
            return new Deserializer();
        }

        private static final long serialVersionUID = 6964040364139601927L;

        public Deserializer() {
            super(ThroughputMeasurementEvent.class);
        }
    
        @Override
        protected ThroughputMeasurementEvent deserializeValue(JsonParser json,
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
            int count = json.getIntValue();
            token = json.nextToken();
            if (token != JsonToken.VALUE_NUMBER_INT) {
                throw ctxt.wrongTokenException(json, JsonToken.VALUE_NUMBER_INT, "");
            }
            int millis = json.getIntValue();
            json.clearCurrentToken();
            ThroughputMeasurementEvent value = new ThroughputMeasurementEvent(count, millis);
            return value;
        }
    }
}
