package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.common.ExecutedActor;

public class TraceWriter extends ExecutedActor<TraceEvent> {

    public static TraceWriter forFile(
            File file,
            ObjectWriter writer,
            Executor executor) throws IOException {
        return create(
                writer.getFactory().createGenerator(file, Trace.ENCODING), 
                writer,
                executor);
    }

    public static TraceWriter create(
            JsonGenerator json,
            ObjectWriter writer,
            Executor executor) {
        return new TraceWriter(
                json,
                writer,
                Queues.<TraceEvent>newConcurrentLinkedQueue(),
                LogManager.getLogger(TraceWriter.class),
                executor);
    }
    
    protected final ObjectWriter writer;
    protected final JsonGenerator json;
    protected final Logger logger;
    protected final Queue<TraceEvent> mailbox;
    protected final Executor executor;
    
    public TraceWriter(
            JsonGenerator json,
            ObjectWriter writer,
            Queue<TraceEvent> mailbox,
            Logger logger, 
            Executor executor) {
        this.writer = writer;
        this.json = json;
        this.logger = logger;
        this.mailbox = mailbox;
        this.executor = executor;

        try {
            json.writeStartArray();
        } catch (IOException e) {
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
    
    @Override
    protected synchronized void doRun() throws Exception {
        TraceEvent next;
        while ((next = mailbox.poll()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }

    @Override
    protected boolean apply(TraceEvent input) throws Exception {
        writer.writeValue(json, TraceEventHeader.create(input));
        return (state() != State.TERMINATED);
    }

    @Override
    protected synchronized void doStop() {
        TraceEvent next;
        while ((next = mailbox.poll()) != null) {
            try {
                apply(next);           
            } catch (Exception e) {
                logger.warn("{}", next, e);
                mailbox.clear();
            }
        }
        
        try {
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
}
