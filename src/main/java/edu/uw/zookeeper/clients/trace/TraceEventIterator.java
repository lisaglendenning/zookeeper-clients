package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

public class TraceEventIterator extends AbstractIterator<TraceEvent> {

    public static Iterator<TraceEvent> forFile(
            File file,
            ObjectReader reader) throws IOException {
        return create(
                reader.getFactory().createParser(file), 
                reader);
    }
    
    public static Iterator<TraceEvent> create(
            JsonParser json,
            ObjectReader reader) throws JsonProcessingException, IOException {
        return new TraceEventIterator(json, reader);
    }
    
    protected final JsonParser json;
    protected final ObjectReader reader;
    
    public TraceEventIterator(
            JsonParser json,
            ObjectReader reader) throws JsonParseException, IOException {
        this.json = json;
        this.reader = reader;
        
        if (! json.hasCurrentToken()) {
            json.nextToken();
        }
        if (! json.isExpectedStartArrayToken()) {
            throw new IllegalArgumentException(String.valueOf(json.getCurrentLocation()));
        }
        json.clearCurrentToken();
    }
    
    @Override
    protected TraceEvent computeNext() {
        if (! json.hasCurrentToken()) {
            try {
                JsonToken next = json.nextToken();
                if ((next == null) || (next == JsonToken.END_ARRAY)) {
                    json.close();
                    return endOfData();
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        try {
            return reader.readValue(json, TraceEventHeader.class).get();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
