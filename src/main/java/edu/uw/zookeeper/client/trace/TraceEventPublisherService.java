package edu.uw.zookeeper.client.trace;

import net.engio.mbassy.bus.common.PubSubSupport;
import net.engio.mbassy.listener.Handler;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.Actor;

public class TraceEventPublisherService extends AbstractIdleService {

    public static TraceEventPublisherService newInstance(
            PubSubSupport<? super TraceEvent> publisher,
            Actor<? super TraceEvent> writer) {
        return new TraceEventPublisherService(publisher, writer);
    }
    
    protected final PubSubSupport<? super TraceEvent> publisher;
    protected final Actor<? super TraceEvent> writer; 
    
    public TraceEventPublisherService(
            PubSubSupport<? super TraceEvent> publisher,
            Actor<? super TraceEvent> writer) {
        this.publisher = publisher;
        this.writer = writer;
    }
    
    public PubSubSupport<? super TraceEvent> getPublisher() {
        return publisher;
    }
    
    public Actor<? super TraceEvent> getWriter() {
        return writer;
    }
    
    @Handler
    public void handleTraceEvent(TraceEvent event) {
        writer.send(event);
    }

    @Override
    protected void startUp() throws Exception {
        publisher.subscribe(this);
        publisher.publish(TimestampEvent.currentTimeMillis());
    }

    @Override
    protected void shutDown() throws Exception {
        publisher.publish(TimestampEvent.currentTimeMillis());
        publisher.unsubscribe(this);
        writer.stop();
    }
}
