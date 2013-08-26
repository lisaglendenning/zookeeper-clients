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

@TraceEventType(TraceEventTag.PROTOCOL_RESPONSE_EVENT)
@JsonSerialize(using=ProtocolResponseEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=ProtocolResponseEvent.Deserializer.class)
public final class ProtocolResponseEvent implements TraceEvent {

    public static ProtocolResponseEvent create(long sessionId, Operation.ProtocolResponse<?> response) {
        return new ProtocolResponseEvent(sessionId, response);
    }

    private final long sessionId;
    private final Operation.ProtocolResponse<?> response;

    public ProtocolResponseEvent(long sessionId, 
            Operation.ProtocolResponse<?> response) {
        this.sessionId = sessionId;
        this.response = checkNotNull(response);
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.PROTOCOL_RESPONSE_EVENT;
    }

    public long getSessionId() {
        return sessionId;
    }
    
    public Operation.ProtocolResponse<?> getResponse() {
        return response;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", sessionId)
                .add("response", response).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof ProtocolResponseEvent)) {
            return false;
        }
        ProtocolResponseEvent other = (ProtocolResponseEvent) obj;
        return (sessionId == other.sessionId)
                && response.equals(other.response);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(sessionId, response);
    }

    public static class Serializer extends StdSerializer<ProtocolResponseEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(ProtocolResponseEvent.class);
        }
    
        @Override
        public void serialize(ProtocolResponseEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeStartArray();
            json.writeNumber(value.sessionId);
            provider.findValueSerializer(value.response.getClass(), null).serialize(value.response, json, provider);
            json.writeEndArray();
        }
    
        @Override
        public JsonNode getSchema(SerializerProvider provider, Type typeHint)
            throws JsonMappingException {
            return createSchemaNode("array");
        }
    }

    public static class Deserializer extends StdDeserializer<ProtocolResponseEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = 8817436001831883720L;
    
        public Deserializer() {
            super(ProtocolResponseEvent.class);
        }
    
        @Override
        public synchronized ProtocolResponseEvent deserialize(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            if (! json.isExpectedStartArrayToken()) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.START_ARRAY.toString());
            }
            json.nextToken();
            long sessionId = json.getLongValue();
            json.nextToken();
            Operation.ProtocolResponse<?> response = (Operation.ProtocolResponse<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolResponse.class), null).deserialize(json, ctxt);
            if (json.getCurrentToken() != JsonToken.END_ARRAY) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.END_ARRAY.toString());
            }
            json.nextToken();
            ProtocolResponseEvent value = new ProtocolResponseEvent(sessionId, response);
            return value;
        }
        
        @Override
        public boolean isCachable() { 
            return true; 
        }
    }
}
