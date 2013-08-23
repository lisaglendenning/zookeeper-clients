package edu.uw.zookeeper.clients.trace;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Message;


public final class LatencyEvent implements TraceEvent {
    
    public static LatencyEvent create(
            long nanos, long sessionId, Message.ClientSession request, Message.ServerSession response) {
        return new LatencyEvent(nanos, sessionId, request, response);
    }

    @JacksonDeserializer
    public static LatencyEvent deserialize(JsonParser parser) throws JsonParseException, IOException {
        return null;
    }

    @JacksonSerializer
    public static void serialize(TimestampEvent value, JsonGenerator generator)
            throws JsonGenerationException, IOException {
    }
    
    private final long nanos;
    private final long sessionId;
    private final Message.ClientSession request;
    private final Message.ServerSession response;
    
    public LatencyEvent(long nanos, long sessionId, Message.ClientSession request, Message.ServerSession response) {
        super();
        this.nanos = nanos;
        this.sessionId = sessionId;
        this.request = request;
        this.response = response;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.LATENCY_EVENT;
    }

    public long getNanos() {
        return nanos;
    }
    
    public long getSessionId() {
        return sessionId;
    }

    public Message.ClientSession getRequest() {
        return request;
    }

    public Message.ServerSession getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("nanos", nanos)
                .add("sessionId", sessionId)
                .add("request", request)
                .add("response", response).toString();
    }
}