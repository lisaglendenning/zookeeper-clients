package edu.uw.zookeeper.client.trace;

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

import edu.uw.zookeeper.protocol.Operation;

@TraceEventType(TraceEventTag.OPERATION_EVENT)
@JsonSerialize(using=OperationEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=OperationEvent.Deserializer.class)
public final class OperationEvent implements TraceEvent {

    public static OperationEvent timeout(
            long sessionId, Operation.ProtocolRequest<?> request) {
        return new OperationEvent(LATENCY_TIMEOUT, sessionId, request, null);
    }
    
    public static OperationEvent create(
            long nanos, long sessionId, Operation.ProtocolRequest<?> request, Operation.ProtocolResponse<?> response) {
        return new OperationEvent(nanos, sessionId, request, response);
    }
    
    public static long LATENCY_TIMEOUT = -1L;
    
    private final long nanos;
    private final long sessionId;
    private final Operation.ProtocolRequest<?> request;
    private final Operation.ProtocolResponse<?> response;
    
    public OperationEvent(long nanos, long sessionId, Operation.ProtocolRequest<?> request, Operation.ProtocolResponse<?> response) {
        super();
        this.nanos = nanos;
        this.sessionId = sessionId;
        this.request = request;
        this.response = response;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.OPERATION_EVENT;
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
        return MoreObjects.toStringHelper(this)
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
        if (! (obj instanceof OperationEvent)) {
            return false;
        }
        OperationEvent other = (OperationEvent) obj;
        return (nanos == other.nanos)
                && (sessionId == other.sessionId)
                && request.equals(other.request)
                && response.equals(other.response);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(nanos, sessionId, request, response);
    }

    public static class Serializer extends ListSerializer<OperationEvent> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(OperationEvent.class);
        }
    
        @Override
        protected void serializeValue(OperationEvent value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            json.writeNumber(value.nanos);
            json.writeNumber(value.sessionId);
            if (value.request == null) {
                json.writeNull();
            } else {
                provider.findValueSerializer(value.request.getClass(), null).serialize(value.request, json, provider);
            }
            if (value.response == null) {
                json.writeNull();
            } else {
                provider.findValueSerializer(value.response.getClass(), null).serialize(value.response, json, provider);
            }
        }
    }

    public static class Deserializer extends ListDeserializer<OperationEvent> {
    
        public static Deserializer create() {
            return new Deserializer();
        }
    
        private static final long serialVersionUID = 8817436001831883720L;
    
        public Deserializer() {
            super(OperationEvent.class);
        }
    
        @Override
        protected OperationEvent deserializeValue(JsonParser json,
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
            long nanos = json.getLongValue();
            token = json.nextToken();
            if (token != JsonToken.VALUE_NUMBER_INT) {
                throw ctxt.wrongTokenException(json, JsonToken.VALUE_NUMBER_INT, "");
            }
            long sessionId = json.getLongValue();
            json.clearCurrentToken();
            Operation.ProtocolRequest<?>request = (token == JsonToken.VALUE_NULL) ? null : (Operation.ProtocolRequest<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolRequest.class), null).deserialize(json, ctxt);
            token = json.getCurrentToken();
            if (token == null) {
                token = json.nextToken();
            }
            Operation.ProtocolResponse<?> response = (token == JsonToken.VALUE_NULL) ? null : (Operation.ProtocolResponse<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolResponse.class), null).deserialize(json, ctxt);
            if (json.hasCurrentToken()) {
                json.clearCurrentToken();
            }
            OperationEvent value = new OperationEvent(nanos, sessionId, request, response);
            return value;
        }
    }
}