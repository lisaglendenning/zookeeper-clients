package edu.uw.zookeeper.client.trace;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

import edu.uw.zookeeper.jackson.JacksonOutputArchive;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;

public class ProtocolResponseHeaderSerializer extends ListSerializer<Message.ServerResponse<?>> {

    public static ProtocolResponseHeaderSerializer create() {
        return new ProtocolResponseHeaderSerializer();
    }
    
    public ProtocolResponseHeaderSerializer() {
        super(Message.ServerResponse.class, true);
    }

    @Override
    protected void serializeValue(Message.ServerResponse<?> value, JsonGenerator json,
            SerializerProvider provider) throws IOException,
            JsonGenerationException {
        json.writeNumber(value.record().opcode().intValue());
        ProtocolResponseMessage.serialize(value, new JacksonOutputArchive(json));
    }
}
