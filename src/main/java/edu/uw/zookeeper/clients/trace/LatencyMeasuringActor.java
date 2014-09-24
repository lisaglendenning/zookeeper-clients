package edu.uw.zookeeper.clients.trace;

import edu.uw.zookeeper.common.Actor;

public class LatencyMeasuringActor implements Actor<TraceEvent> {

    public static LatencyMeasuringActor create(
            Actor<? super TraceEvent> delegate) {
        return new LatencyMeasuringActor(delegate);
    }
    
    protected final Actor<? super TraceEvent> delegate;
    
    public LatencyMeasuringActor(
            Actor<? super TraceEvent> delegate) {
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
        if (message instanceof OperationEvent) {
            OperationEvent operation = (OperationEvent) message;
            long latency = Math.max(0L, operation.getResponseNanos() - operation.getRequestNanos());
            LatencyMeasurementEvent event = LatencyMeasurementEvent.fromNanos(latency);            delegate.send(event);
        }
        return delegate.send(message);
    }

    @Override
    public boolean stop() {
        return delegate.stop();
    }
}
