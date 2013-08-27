package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public abstract class ListSerializer<T> extends StdSerializer<T> {

    protected ListSerializer(Class<?> t, boolean dummy) {
        super(t, dummy);
    }

    protected ListSerializer(Class<T> t) {
        super(t);
    }

    @Override
    public void serialize(T value, JsonGenerator json,
            SerializerProvider provider) throws IOException,
            JsonGenerationException {
        json.writeStartArray();
        serializeValue(value, json, provider);
        json.writeEndArray();
    }

    @Override
    public JsonNode getSchema(SerializerProvider provider, Type typeHint)
        throws JsonMappingException {
        return createSchemaNode("array");
    }

    protected abstract void serializeValue(T value, JsonGenerator json,
            SerializerProvider provider) throws IOException,
            JsonGenerationException;
}
