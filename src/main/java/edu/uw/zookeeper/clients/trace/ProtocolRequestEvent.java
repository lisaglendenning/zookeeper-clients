package edu.uw.zookeeper.clients.trace;

import static com.google.common.base.Preconditions.checkNotNull;

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

@TraceEventType(TraceEventTag.PROTOCOL_REQUEST_EVENT)
@JsonSerialize(using=ProtocolRequestEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=ProtocolRequestEvent.Deserializer.class)
public final class ProtocolRequestEvent implements TraceEvent {

    public static ProtocolRequestEvent create(long sessionId, Operation.ProtocolRequest<?> request) {
        return new ProtocolRequestEvent(sessionId, request);
    }

    private final long sessionId;
    private final Operation.ProtocolRequest<?> request;

    public ProtocolRequestEvent(long sessionId, 
            Operation.ProtocolRequest<?> request) {
        this.sessionId = sessionId;
        this.request = checkNotNull(request);
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.PROTOCOL_REQUEST_EVENT;
    }

    public long getSessionId() {
        return sessionId;
    }
    
    public Operation.ProtocolRequest<?> getRequest() {
        return request;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", sessionId)
                .add("request", request).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof ProtocolRequestEvent)) {
            return false;
        }
        ProtocolRequestEvent other = (ProtocolRequestEvent) obj;
        return (sessionId == other.sessionId)
                && request.equals(other.request);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(sessionId, request);
    }

    public static class Serializer extends StdSerializer<ProtocolRequestEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(ProtocolRequestEvent.class);
        }
    
        @Override
        public void serialize(ProtocolRequestEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeStartArray();
            json.writeNumber(value.sessionId);
            provider.findValueSerializer(value.request.getClass(), null).serialize(value.request, json, provider);
            json.writeEndArray();
        }
    
        @Override
        public JsonNode getSchema(SerializerProvider provider, Type typeHint)
            throws JsonMappingException {
            return createSchemaNode("array");
        }
    }

    public static class Deserializer extends StdDeserializer<ProtocolRequestEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = -8349734923955508574L;
    
        public Deserializer() {
            super(ProtocolRequestEvent.class);
        }
    
        @Override
        public ProtocolRequestEvent deserialize(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            if (! json.isExpectedStartArrayToken()) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.START_ARRAY.toString());
            }
            json.nextToken();
            long sessionId = json.getLongValue();
            json.nextToken();
            Operation.ProtocolRequest<?> request = (Operation.ProtocolRequest<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolRequest.class), null).deserialize(json, ctxt);
            if (json.getCurrentToken() != JsonToken.END_ARRAY) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.END_ARRAY.toString());
            }
            json.nextToken();
            ProtocolRequestEvent value = new ProtocolRequestEvent(sessionId, request);
            return value;
        }
        
        @Override
        public boolean isCachable() { 
            return true; 
        }
    }
}
