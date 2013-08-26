package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import edu.uw.zookeeper.jackson.JacksonOutputArchive;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;

public class ProtocolResponseHeaderSerializer extends StdSerializer<Operation.ProtocolResponse<?>> {

    public static ProtocolResponseHeaderSerializer create() {
        return new ProtocolResponseHeaderSerializer();
    }
    
    public ProtocolResponseHeaderSerializer() {
        super(Operation.ProtocolResponse.class, true);
    }

    @Override
    public void serialize(Operation.ProtocolResponse<?> value, JsonGenerator json,
            SerializerProvider provider) throws IOException,
            JsonGenerationException {
        json.writeStartArray();
        json.writeNumber(value.record().opcode().intValue());
        JacksonOutputArchive archive = new JacksonOutputArchive(json);
        ProtocolResponseMessage.serialize(value, archive);
        json.writeEndArray();
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
        return createSchemaNode("array");
    }
}
