package edu.uw.zookeeper.clients.trace;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.proto.OpCode;

public class ThroughputMeasuringActor implements Actor<TraceEvent> {

    public static ThroughputMeasuringActor create(
            Configuration configuration,
            Actor<? super TraceEvent> delegate) {
        int interval = (int) ConfigurableThroughputInterval.get(configuration).value(TimeUnit.MILLISECONDS);
        return new ThroughputMeasuringActor(interval, delegate);
    }
    
    public static ThroughputMeasuringActor create(
            int interval,
            Actor<? super TraceEvent> delegate) {
        return new ThroughputMeasuringActor(interval, delegate);
    }
    
    @Configurable(arg="interval", key="throughputInterval", value="500 ms", help="time")
    public static class ConfigurableThroughputInterval implements Function<Configuration, TimeValue> {
    
        public static TimeValue get(Configuration configuration) {
            return new ConfigurableThroughputInterval().apply(configuration);
        }
    
        @Override
        public TimeValue apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }
    }

    protected final long interval;
    protected long start;
    protected int count;
    protected final Actor<? super TraceEvent> delegate;
    
    public ThroughputMeasuringActor(
            int interval,
            Actor<? super TraceEvent> delegate) {
        this.interval = TimeUnit.NANOSECONDS.convert(interval, TimeUnit.MILLISECONDS);
        this.delegate = delegate;
        this.count = 0;
        this.start = 0;
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
            if ((operation.getRequest() != null) && (operation.getResponse() != null) && (operation.getRequest().record().opcode() != OpCode.CLOSE_SESSION)) {
                ImmutableList.Builder<TraceEvent> events = ImmutableList.builder();
                synchronized (this) {
                    if (start == 0) {
                        start = operation.getRequestNanos();
                    }
                    // complete preceding intervals
                    long duration = Math.max(0L, operation.getResponseNanos() - start);
                    while (duration > interval) {
                        events.add(ThroughputMeasurementEvent.fromNanos(count, interval));
                        if (count != 0) {
                            count = 0;
                        }
                        start += interval;
                        duration -= interval;
                    }
                    count += 1;
                    // complete this interval
                    if (duration == interval) {
                        events.add(ThroughputMeasurementEvent.fromNanos(count, interval));
                        if (count != 0) {
                            count = 0;
                        }
                        start += interval;
                        duration = 0;
                    }
                }
                for (TraceEvent event: events.build()) {
                    delegate.send(event);
                }
            }
        }
        return delegate.send(message);
    }

    @Override
    public boolean stop() {
        ThroughputMeasurementEvent event = null;
        synchronized (this) {
            if (count > 0) {
                assert (start > 0);
                long duration = System.nanoTime() - start;
                event = ThroughputMeasurementEvent.fromNanos(count, duration);
                start = 0;
                count = 0;
            }
        }
        if (event != null) {
            delegate.send(event);
        }
        return delegate.stop();
    }
}
