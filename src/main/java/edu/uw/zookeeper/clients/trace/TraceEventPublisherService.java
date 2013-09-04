package edu.uw.zookeeper.clients.trace;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Publisher;

public class TraceEventPublisherService extends AbstractIdleService {

    public static TraceEventPublisherService newInstance(
            Publisher publisher,
            Actor<? super TraceEvent> writer) {
        return new TraceEventPublisherService(publisher, writer);
    }
    
    protected final Publisher publisher;
    protected final Actor<? super TraceEvent> writer; 
    
    public TraceEventPublisherService(
            Publisher publisher,
            Actor<? super TraceEvent> writer) {
        this.publisher = publisher;
        this.writer = writer;
    }
    
    public Publisher getPublisher() {
        return publisher;
    }
    
    public Actor<? super TraceEvent> getWriter() {
        return writer;
    }
    
    @Subscribe
    public void handleTraceEvent(TraceEvent event) {
        writer.send(event);
    }

    @Override
    protected void startUp() throws Exception {
        publisher.register(this);
        publisher.post(TimestampEvent.currentTimeMillis());
    }

    @Override
    protected void shutDown() throws Exception {
        publisher.post(TimestampEvent.currentTimeMillis());
        publisher.unregister(this);
        writer.stop();
    }
}
