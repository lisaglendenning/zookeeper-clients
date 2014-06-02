package edu.uw.zookeeper.client.trace;

import com.google.common.base.Predicate;

import edu.uw.zookeeper.common.Actor;

public class FilteringTraceEventActor implements Actor<TraceEvent> {

    public static FilteringTraceEventActor create(
            Predicate<? super TraceEvent> filter,
            Actor<? super TraceEvent> delegate) {
        return new FilteringTraceEventActor(filter, delegate);
    }
    
    protected final Predicate<? super TraceEvent> filter;
    protected final Actor<? super TraceEvent> delegate;
    
    public FilteringTraceEventActor(
            Predicate<? super TraceEvent> filter,
            Actor<? super TraceEvent> delegate) {
        this.filter = filter;
        this.delegate = delegate;
    }
    
    @Override
    public void run() {
        delegate.run();
    }

    @Override
    public State state() {
        return delegate.state();
    }

    @Override
    public boolean send(TraceEvent message) {
        if (filter.apply(message)) {
            return delegate.send(message);
        } else {
            return false;
        }
    }

    @Override
    public boolean stop() {
        return delegate.stop();
    }
}
