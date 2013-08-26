package edu.uw.zookeeper.clients.trace;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uw.zookeeper.jackson.JacksonInputArchive;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;

public class ProtocolResponseHeaderDeserializer extends StdDeserializer<Operation.ProtocolResponse<?>> {

    public static ProtocolResponseHeaderDeserializer create() {
        return new ProtocolResponseHeaderDeserializer();
    }
    
    private static final long serialVersionUID = -8230711507540547370L;

    public ProtocolResponseHeaderDeserializer() {
        super(Operation.ProtocolResponse.class);
    }

    @Override
    public Operation.ProtocolResponse<?> deserialize(JsonParser json,
            DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        if (! json.isExpectedStartArrayToken()) {
            throw new JsonParseException(String.valueOf(json.getCurrentToken()), json.getCurrentLocation());
        }
        json.nextToken();
        OpCode opcode = OpCode.of(json.getIntValue());
        json.nextToken();
        JacksonInputArchive archive = new JacksonInputArchive(json);
        ProtocolResponseMessage<?> value = ProtocolResponseMessage.deserialize(opcode, archive);
        if (json.getCurrentToken() != JsonToken.END_ARRAY) {
            throw new JsonParseException(String.valueOf(json.getCurrentToken()), json.getCurrentLocation());
        }
        json.nextToken();
        return value;
    }

    @Override
    public boolean isCachable() { 
        return true; 
    }
}
