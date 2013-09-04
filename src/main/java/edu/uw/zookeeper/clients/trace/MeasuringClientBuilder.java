package edu.uw.zookeeper.clients.trace;

import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.inject.internal.ImmutableSet;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.random.ConstantGenerator;
import edu.uw.zookeeper.clients.random.PathedRequestGenerator;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;

public class MeasuringClientBuilder extends TraceGeneratingClientBuilder<MeasuringClientBuilder> {

    public static MeasuringClientBuilder defaults() {
        return new MeasuringClientBuilder();
    }
    
    @Configurable(arg="latency", key="MeasureLatency", value="false", type=ConfigValueType.BOOLEAN)
    public static class MeasureLatencyConfiguration implements Function<Configuration, Boolean> {

        public static Boolean get(Configuration configuration) {
            return new MeasureLatencyConfiguration().apply(configuration);
        }

        @Override
        public Boolean apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getBoolean(configurable.key());
        }
    }

    @Configurable(arg="throughput", key="MeasureThroughput", value="true", type=ConfigValueType.BOOLEAN)
    public static class MeasureThroughputConfiguration implements Function<Configuration, Boolean> {

        public static Boolean get(Configuration configuration) {
            return new MeasureThroughputConfiguration().apply(configuration);
        }

        @Override
        public Boolean apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getBoolean(configurable.key());
        }
    }

    protected MeasuringClientBuilder() {
        this(null, null, null, null, null);
    }

    protected MeasuringClientBuilder(ObjectMapper mapper,
            TraceWriterBuilder traceBuilder,
            TraceEventPublisherService tracePublisher,
            ClientBuilder clientBuilder, RuntimeModule runtime) {
        super(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    @Override
    protected MeasuringClientBuilder newInstance(ObjectMapper mapper,
            TraceWriterBuilder traceBuilder,
            TraceEventPublisherService tracePublisher,
            ClientBuilder clientBuilder, RuntimeModule runtime) {
        return new MeasuringClientBuilder(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    @Override
    protected TraceHeader getDefaultTraceHeader() {
        Configuration configuration = getRuntimeModule().getConfiguration();
        ImmutableSet.Builder<TraceEventTag> types = ImmutableSet.builder();
        types.add(TraceEventTag.TIMESTAMP_EVENT);
        if (MeasureLatencyConfiguration.get(configuration)) {
            types.add(TraceEventTag.LATENCY_MEASUREMENT_EVENT);
        }
        if (MeasureThroughputConfiguration.get(configuration)) {
            types.add(TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT);
        }
        if (types.build().size() == 1) {
            types.add(TraceEventTag.OPERATION_EVENT);
        }
        return TraceHeader.create(
                Trace.getTraceDescription(getRuntimeModule().getConfiguration()), 
                types.build());
    }
    
    @Override
    protected TraceEventPublisherService getDefaultTraceEventPublisherService() {
        TraceWriter writer = traceBuilder.build();
        final Set<TraceEventTag> types = writer.header().getTypes();
        Predicate<TraceEvent> filter = new Predicate<TraceEvent>() {
            @Override
            public boolean apply(TraceEvent input) {
                return types.contains(input.getTag());
            }
        };
        Actor<TraceEvent> actor = FilteringTraceEventActor.create(
                filter, writer);
        if (types.contains(TraceEventTag.LATENCY_MEASUREMENT_EVENT)) {
            actor = LatencyMeasuringActor.create(actor);
        }
        if (types.contains(TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT)) {
            actor = ThroughputMeasuringActor.create(
                    getRuntimeModule().getConfiguration(), actor);
        }
        return TraceEventPublisherService.newInstance(
                EventBusPublisher.newInstance(), 
                actor);
    }
    
    @Override
    protected ClientBuilder getDefaultClientBuilder() {
        return ClientBuilder.defaults()
                .setConnectionBuilder(ClientConnectionFactoryBuilder.defaults()
                        .setCodecFactory(OperationTracingCodec.factory(tracePublisher.getPublisher())))
                .setRuntimeModule(getRuntimeModule())
                .setDefaults();
    }

    @Override
    protected Generator<Records.Request> getDefaultRequestGenerator() {
        return PathedRequestGenerator.exists(ConstantGenerator.of(ZNodeLabel.Path.root()));
    }
}
