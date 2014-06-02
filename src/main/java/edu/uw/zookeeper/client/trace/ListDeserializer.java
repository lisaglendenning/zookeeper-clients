package edu.uw.zookeeper.client.trace;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

@SuppressWarnings("serial")
public abstract class ListDeserializer<T> extends StdDeserializer<T> {

    protected ListDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public T deserialize(JsonParser json,
            DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        JsonToken token = json.getCurrentToken();
        if (token == null) {
            token = json.nextToken();
            if (token == null) {
                return null;
            }
        }
        if (! json.isExpectedStartArrayToken()) {
            throw ctxt.wrongTokenException(json, JsonToken.START_ARRAY, "");
        }
        json.clearCurrentToken();
        
        T value = deserializeValue(json, ctxt);

        token = json.getCurrentToken();
        if (token == null) {
            token = json.nextToken();
        }
        if (token != JsonToken.END_ARRAY) {
            throw ctxt.wrongTokenException(json, JsonToken.END_ARRAY, "");
        }
        json.clearCurrentToken();
        
        return value;
    }
    
    @Override
    public boolean isCachable() { 
        return true; 
    }
    
    protected abstract T deserializeValue(JsonParser json,
            DeserializationContext ctxt) throws IOException,
            JsonProcessingException;
}
