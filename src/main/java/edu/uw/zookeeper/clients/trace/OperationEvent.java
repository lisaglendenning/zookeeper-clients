package edu.uw.zookeeper.clients.trace;

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
import com.google.common.base.Objects;
import edu.uw.zookeeper.protocol.Operation;

@TraceEventType(TraceEventTag.OPERATION_EVENT)
@JsonSerialize(using=OperationEvent.Serializer.class, typing=JsonSerialize.Typing.STATIC)
@JsonDeserialize(using=OperationEvent.Deserializer.class)
public final class OperationEvent implements TraceEvent {

    public static OperationEvent timeout(
            long sessionId, Operation.ProtocolRequest<?> request) {
        return new OperationEvent(sessionId, request, TIMEOUT, null, TIMEOUT);
    }
    
    public static OperationEvent create(
            long sessionId, 
            Operation.ProtocolRequest<?> request, 
            long requestNanos,
            Operation.ProtocolResponse<?> response,
            long responseNanos) {
        return new OperationEvent(sessionId, request, requestNanos, response, responseNanos);
    }
    
    public static long TIMEOUT = -1L;
    
    private final long sessionId;
    private final Operation.ProtocolRequest<?> request;
    private final long requestNanos;
    private final Operation.ProtocolResponse<?> response;
    private final long responseNanos;
    
    public OperationEvent(
            long sessionId, 
            Operation.ProtocolRequest<?> request, 
            long requestNanos,
            Operation.ProtocolResponse<?> response,
            long responseNanos) {
        super();
        this.sessionId = sessionId;
        this.request = request;
        this.requestNanos = requestNanos;
        this.response = response;
        this.responseNanos = responseNanos;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.OPERATION_EVENT;
    }
    
    public long getSessionId() {
        return sessionId;
    }

    public Operation.ProtocolRequest<?> getRequest() {
        return request;
    }

    public long getRequestNanos() {
        return requestNanos;
    }

    public Operation.ProtocolResponse<?> getResponse() {
        return response;
    }

    public long getResponseNanos() {
        return responseNanos;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", sessionId)
                .add("request", request)
                .add("requestNanos", requestNanos)
                .add("response", response)
                .add("responseNanos", responseNanos).toString();
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
        return (sessionId == other.sessionId)
                && request.equals(other.request)
                && (requestNanos == other.requestNanos)
                && response.equals(other.response)
                && (responseNanos == other.responseNanos);
    }
    
    @Override
    public int hashCode() {
        return (int) requestNanos;
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
            json.writeNumber(value.sessionId);
            if (value.request == null) {
                json.writeNull();
            } else {
                provider.findValueSerializer(value.request.getClass(), null).serialize(value.request, json, provider);
                json.writeNumber(value.requestNanos);
            }
            if (value.response == null) {
                json.writeNull();
            } else {
                provider.findValueSerializer(value.response.getClass(), null).serialize(value.response, json, provider);
                json.writeNumber(value.responseNanos);
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
            long sessionId = json.getLongValue();
            json.clearCurrentToken();
            Operation.ProtocolRequest<?> request = (token == JsonToken.VALUE_NULL) ? null : (Operation.ProtocolRequest<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolRequest.class), null).deserialize(json, ctxt);
            token = json.getCurrentToken();
            if (token == null) {
                token = json.nextToken();
            }
            long requestNanos;
            if (request != null) {
                if (token != JsonToken.VALUE_NUMBER_INT) {
                    throw ctxt.wrongTokenException(json, JsonToken.VALUE_NUMBER_INT, "");
                }
                requestNanos = json.getLongValue();
                token = json.nextToken();
            } else {
                requestNanos = TIMEOUT;
            }
            Operation.ProtocolResponse<?> response = (token == JsonToken.VALUE_NULL) ? null : (Operation.ProtocolResponse<?>) ctxt.findContextualValueDeserializer(ctxt.constructType(Operation.ProtocolResponse.class), null).deserialize(json, ctxt);
            if (json.hasCurrentToken()) {
                json.clearCurrentToken();
            }
            long responseNanos;
            if (response != null) {
                if (token != JsonToken.VALUE_NUMBER_INT) {
                    throw ctxt.wrongTokenException(json, JsonToken.VALUE_NUMBER_INT, "");
                }
                responseNanos = json.getLongValue();
                token = json.nextToken();
            } else {
                responseNanos = TIMEOUT;
            }
            OperationEvent value = new OperationEvent(sessionId, request, requestNanos, response, responseNanos);
            return value;
        }
    }
}
