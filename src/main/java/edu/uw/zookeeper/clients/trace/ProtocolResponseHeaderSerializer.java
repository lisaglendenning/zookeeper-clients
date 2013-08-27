package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import edu.uw.zookeeper.jackson.JacksonOutputArchive;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;

public class ProtocolResponseHeaderSerializer extends ListSerializer<Operation.ProtocolResponse<?>> {

    public static ProtocolResponseHeaderSerializer create() {
        return new ProtocolResponseHeaderSerializer();
    }
    
    public ProtocolResponseHeaderSerializer() {
        super(Operation.ProtocolResponse.class, true);
    }

    @Override
    protected void serializeValue(Operation.ProtocolResponse<?> value, JsonGenerator json,
            SerializerProvider provider) throws IOException,
            JsonGenerationException {
        json.writeNumber(value.record().opcode().intValue());
        ProtocolResponseMessage.serialize(value,  new JacksonOutputArchive(json));
    }
}
