package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.Type;

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

import edu.uw.zookeeper.protocol.Operation;

@TraceEventType(TraceEventTag.LATENCY_EVENT)
@JsonSerialize(using=LatencyEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=LatencyEvent.Deserializer.class)
public final class LatencyEvent implements TraceEvent {
    
    public static LatencyEvent create(
            long nanos, long sessionId, Operation.ProtocolRequest<?> request, Operation.ProtocolResponse<?> response) {
        return new LatencyEvent(nanos, sessionId, request, response);
    }
    
    private final long nanos;
    private final long sessionId;
    private final Operation.ProtocolRequest<?> request;
    private final Operation.ProtocolResponse<?> response;
    
    public LatencyEvent(long nanos, long sessionId, Operation.ProtocolRequest<?> request, Operation.ProtocolResponse<?> response) {
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

    public Operation.ProtocolRequest<?> getRequest() {
        return request;
    }

    public Operation.ProtocolResponse<?> getResponse() {
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
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof LatencyEvent)) {
            return false;
        }
        LatencyEvent other = (LatencyEvent) obj;
        return (nanos == other.nanos)
                && (sessionId == other.sessionId)
                && request.equals(other.request)
                && response.equals(other.response);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(nanos, sessionId, request, response);
    }

    public static class Serializer extends StdSerializer<LatencyEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(LatencyEvent.class);
        }
    
        @Override
        public void serialize(LatencyEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeStartArray();
            json.writeNumber(value.nanos);
            json.writeNumber(value.sessionId);
            provider.findValueSerializer(value.request.getClass(), null).serialize(value.request, json, provider);
            provider.findValueSerializer(value.response.getClass(), null).serialize(value.response, json, provider);
            json.writeEndArray();
        }
    
        @Override
        public JsonNode getSchema(SerializerProvider provider, Type typeHint)
            throws JsonMappingException {
            return createSchemaNode("array");
        }
    }

    public static class Deserializer extends StdDeserializer<LatencyEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = 8817436001831883720L;
    
        public Deserializer() {
            super(LatencyEvent.class);
        }
    
        @Override
        public synchronized LatencyEvent deserialize(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            if (! json.isExpectedStartArrayToken()) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.START_ARRAY.toString());
            }
            json.nextToken();
            long nanos = json.getLongValue();
            json.nextToken();
            long sessionId = json.getLongValue();
            json.nextToken();
            Operation.ProtocolRequest<?>request = (Operation.ProtocolRequest<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolRequest.class), null).deserialize(json, ctxt);
            Operation.ProtocolResponse<?> response = (Operation.ProtocolResponse<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolResponse.class), null).deserialize(json, ctxt);
            if (json.getCurrentToken() != JsonToken.END_ARRAY) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.END_ARRAY.toString());
            }
            json.nextToken();
            LatencyEvent value = new LatencyEvent(nanos, sessionId, request, response);
            return value;
        }
        
        @Override
        public boolean isCachable() { 
            return true; 
        }
    }
}