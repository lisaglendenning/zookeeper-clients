package edu.uw.zookeeper.clients.trace;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Actor;

public class TraceEventSubscriber {

    protected final Actor<? super TraceEvent> sink; 
    
    public TraceEventSubscriber(
            Actor<? super TraceEvent> sink) {
        this.sink = sink;
    }
    
    @Subscribe
    public void handleTraceEvent(TraceEvent event) {
        sink.send(event);
    }
}
