package edu.uw.zookeeper.client.trace;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

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

import edu.uw.zookeeper.protocol.Message;

@TraceEventType(TraceEventTag.PROTOCOL_RESPONSE_EVENT)
@JsonSerialize(using=ProtocolResponseEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=ProtocolResponseEvent.Deserializer.class)
public final class ProtocolResponseEvent implements TraceEvent {

    public static ProtocolResponseEvent create(long sessionId, Message.ServerResponse<?> response) {
        return new ProtocolResponseEvent(sessionId, response);
    }

    private final long sessionId;
    private final Message.ServerResponse<?> response;

    public ProtocolResponseEvent(long sessionId, 
            Message.ServerResponse<?> response) {
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
    
    public Message.ServerResponse<?> getResponse() {
        return response;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
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

    public static class Serializer extends ListSerializer<ProtocolResponseEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(ProtocolResponseEvent.class);
        }
    
        @Override
        protected void serializeValue(ProtocolResponseEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeNumber(value.sessionId);
            provider.findValueSerializer(value.response.getClass(), null).serialize(value.response, json, provider);
        }
    }

    public static class Deserializer extends ListDeserializer<ProtocolResponseEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = 8817436001831883720L;
    
        public Deserializer() {
            super(ProtocolResponseEvent.class);
        }
    
        @Override
        protected ProtocolResponseEvent deserializeValue(JsonParser json,
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
            long sessionId = json.getLongValue();
            json.clearCurrentToken();
            Message.ServerResponse<?> response = (Message.ServerResponse<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Message.ServerResponse.class), null).deserialize(json, ctxt);
            if (json.hasCurrentToken()) {
                json.clearCurrentToken();
            }
            ProtocolResponseEvent value = new ProtocolResponseEvent(sessionId, response);
            return value;
        }
    }
}
