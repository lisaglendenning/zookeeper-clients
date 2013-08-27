package edu.uw.zookeeper.clients.trace;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.Publisher;

public class TraceEventWriterService extends AbstractIdleService {

    public static TraceEventWriterService newInstance(
            Publisher publisher,
            TraceWriter writer) {
        return new TraceEventWriterService(publisher, writer);
    }
    
    protected final Publisher publisher;
    protected final TraceWriter writer; 
    
    public TraceEventWriterService(
            Publisher publisher,
            TraceWriter writer) {
        this.publisher = publisher;
        this.writer = writer;
    }
    
    @Subscribe
    public void handleTraceEvent(TraceEvent event) {
        writer.send(event);
    }

    @Override
    protected void startUp() throws Exception {
        publisher.register(this);
    }

    @Override
    protected void shutDown() throws Exception {
        publisher.unregister(this);
        writer.stop();
    }
}
