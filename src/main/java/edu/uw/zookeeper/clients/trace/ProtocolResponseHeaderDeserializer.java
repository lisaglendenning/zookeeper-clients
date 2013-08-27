package edu.uw.zookeeper.clients.trace;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import edu.uw.zookeeper.jackson.JacksonInputArchive;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;

public class ProtocolResponseHeaderDeserializer extends ListDeserializer<Operation.ProtocolResponse<?>> {

    public static ProtocolResponseHeaderDeserializer create() {
        return new ProtocolResponseHeaderDeserializer();
    }
    
    private static final long serialVersionUID = -8230711507540547370L;

    public ProtocolResponseHeaderDeserializer() {
        super(Operation.ProtocolResponse.class);
    }

    @Override
    protected Operation.ProtocolResponse<?> deserializeValue(JsonParser json,
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
        OpCode opcode = OpCode.of(json.getIntValue());
        json.clearCurrentToken();
        return ProtocolResponseMessage.deserialize(opcode, new JacksonInputArchive(json));
    }
}
