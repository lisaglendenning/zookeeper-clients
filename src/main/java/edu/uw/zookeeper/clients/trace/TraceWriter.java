package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.common.Actors.ExecutedQueuedActor;

public class TraceWriter extends ExecutedQueuedActor<TraceEvent> {

    public static TraceWriter forFile(
            File file,
            ObjectWriter writer,
            TraceHeader header,
            Executor executor) throws IOException {
        return create(
                writer.getFactory().createGenerator(file, Tracing.ENCODING), 
                writer,
                header,
                executor);
    }

    public static TraceWriter create(
            JsonGenerator json,
            ObjectWriter writer,
            TraceHeader header,
            Executor executor) throws IOException {
        return new TraceWriter(
                json,
                writer,
                header,
                Queues.<TraceEvent>newConcurrentLinkedQueue(),
                LogManager.getLogger(TraceWriter.class),
                executor);
    }
    
    protected final ObjectWriter writer;
    protected final JsonGenerator json;
    protected final Logger logger;
    protected final Queue<TraceEvent> mailbox;
    protected final Executor executor;
    protected final TraceHeader header;
    
    public TraceWriter(
            JsonGenerator json,
            ObjectWriter writer,
            TraceHeader header,
            Queue<TraceEvent> mailbox,
            Logger logger, 
            Executor executor) throws IOException {
        this.writer = writer;
        this.json = json;
        this.header = header;
        this.logger = logger;
        this.mailbox = mailbox;
        this.executor = executor;

        json.writeStartArray();
        writer.writeValue(json, header);
        json.writeStartArray();
    }
    
    public TraceHeader header() {
        return header;
    }
    
    @Override
    protected synchronized void doRun() {
        TraceEvent next;
        while ((next = mailbox.poll()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }

    @Override
    protected boolean apply(TraceEvent input) {
        try {
            writer.writeValue(json, TraceEventHeader.create(input));
            return true;
        } catch (Exception e) {
            logger.warn("{}", input, e);
            mailbox.clear();
            stop();
            return false;
        }
    }

    @Override
    protected synchronized void doStop() {
        doRun();
        
        try {
            json.writeEndArray();
            json.writeEndArray();
        } catch (IOException e) {
        }
        
        try {
            json.flush();
        } catch (IOException e) {
        } finally {
            try {
                json.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    @Override
    protected Queue<TraceEvent> mailbox() {
        return mailbox;
    }
}
