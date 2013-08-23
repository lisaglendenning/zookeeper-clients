package edu.uw.zookeeper.clients.trace;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.base.Objects;

@TraceEventType(TraceEventTag.TIMESTAMP_EVENT)
public final class TimestampEvent implements TraceEvent {

    public static TimestampEvent currentTimeMillis() {
        return new TimestampEvent(System.currentTimeMillis());
    }
    
    @JacksonDeserializer
    public static TimestampEvent deserialize(JsonParser parser) throws JsonParseException, IOException {
        long timestamp = parser.getLongValue();
        return new TimestampEvent(timestamp);
    }

    @JacksonSerializer
    public static void serialize(TimestampEvent value, JsonGenerator generator)
            throws JsonGenerationException, IOException {
        generator.writeNumber(value.timestamp);
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
}
