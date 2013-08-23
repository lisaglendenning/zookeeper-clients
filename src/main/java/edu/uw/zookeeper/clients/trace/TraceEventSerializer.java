package edu.uw.zookeeper.clients.trace;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

public class TraceEventSerializer {

    public static TraceEventSerializer create() {
        return new TraceEventSerializer();
    }
    
    public TraceEventSerializer() {
    }
    
    public TraceEventSerializer serialize(JsonGenerator generator, TraceEvent event) throws JsonGenerationException, IOException {
        TraceEventTag tag = event.getTag();
        generator.writeNumber(tag.ordinal());
        switch (tag) {
            default:
                break;
        }
        return this;
    }
}
