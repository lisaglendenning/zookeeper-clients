package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.common.ExecutedActor;

public class TraceWriter extends ExecutedActor<TraceEvent> {

    public static TraceWriter forFile(
            File file,
            JsonFactory factory, 
            TraceEventSerializer generator,
            Executor executor) throws IOException {
        return create(
                factory.createGenerator(file, ENCODING), 
                generator,
                executor);
    }
    
    /**
     * Will not automatically close stream
     */
    public static TraceWriter forStream(
            OutputStream stream,
            JsonFactory factory, 
            TraceEventSerializer generator,
            Executor executor) throws IOException {
        return create(
                factory.createGenerator(stream, ENCODING), 
                generator,
                executor);
    }

    public static TraceWriter create(
            JsonGenerator json,
            TraceEventSerializer generator,
            Executor executor) {
        return new TraceWriter(
                json,
                generator, 
                Queues.<TraceEvent>newConcurrentLinkedQueue(),
                LogManager.getLogger(TraceWriter.class),
                executor);
    }
    
    public static JsonEncoding ENCODING = JsonEncoding.UTF8;
    
    protected final JsonGenerator json;
    protected final TraceEventSerializer generator;
    protected final Logger logger;
    protected final Queue<TraceEvent> mailbox;
    protected final Executor executor;
    
    public TraceWriter(
            JsonGenerator json,
            TraceEventSerializer generator,
            Queue<TraceEvent> mailbox,
            Logger logger, 
            Executor executor) {
        this.json = json;
        this.generator = generator;
        this.logger = logger;
        this.mailbox = mailbox;
        this.executor = executor;
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
        generator.serialize(json, input);
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
