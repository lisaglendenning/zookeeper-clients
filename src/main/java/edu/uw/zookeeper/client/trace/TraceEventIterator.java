package edu.uw.zookeeper.client.trace;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

public class TraceEventIterator extends AbstractIterator<TraceEvent> implements Closeable {

    public static TraceEventIterator forFile(
            File file,
            ObjectReader reader) throws IOException {
        return create(
                reader.getFactory().createParser(file), 
                reader);
    }
    
    public static TraceEventIterator create(
            JsonParser json,
            ObjectReader reader) throws JsonProcessingException, IOException {
        return new TraceEventIterator(json, reader);
    }
    
    protected final JsonParser json;
    protected final ObjectReader reader;
    protected final TraceHeader header;
    
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
        json.nextToken();
        this.header = reader.readValue(json, TraceHeader.class);
        json.nextToken();
        if (! json.isExpectedStartArrayToken()) {
            throw new IllegalArgumentException(String.valueOf(json.getCurrentLocation()));
        }
        json.clearCurrentToken();
    }
    
    public TraceHeader header() {
        return header;
    }
    
    @Override
    protected TraceEvent computeNext() {
        if (! json.hasCurrentToken()) {
            try {
                JsonToken next = json.nextToken();
                if ((next == null) || (next == JsonToken.END_ARRAY)) {
                    close();
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

    @Override
    public void close() throws IOException {
        json.close();
    }
}
