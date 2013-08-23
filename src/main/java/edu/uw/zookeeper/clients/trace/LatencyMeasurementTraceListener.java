package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Processors;

public class LatencyMeasurementTraceListener<O> {

    protected final Processors.UncheckedProcessor<? super LatencyEvent, Optional<O>> processor;
    protected final Actor<? super O> sink; 
    
    public LatencyMeasurementTraceListener(
            Processors.UncheckedProcessor<? super LatencyEvent, Optional<O>> processor,
            Actor<? super O> sink) {
        this.processor = processor;
        this.sink = sink;
    }
    
    @Subscribe
    public void handleLatencyMeasurement(LatencyEvent event) {
        Optional<O> output = processor.apply(event);
        if (output.isPresent()) {
            sink.send(output.get());
        }
    }
    
    public static class LatencyEventToMeasurement implements Processors.UncheckedProcessor<LatencyEvent, Optional<LatencyMeasurementEvent>> {
        @Override
        public Optional<LatencyMeasurementEvent> apply(LatencyEvent input) {
            return Optional.of(LatencyMeasurementEvent.from(input));
        }
    }
}
