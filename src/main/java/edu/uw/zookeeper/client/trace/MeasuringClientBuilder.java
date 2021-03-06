package edu.uw.zookeeper.client.trace;

import java.util.Set;

import net.engio.mbassy.bus.BusFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Generators;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.proto.Records;

public class MeasuringClientBuilder extends TraceGeneratingClientBuilder<MeasuringClientBuilder> {

    public static MeasuringClientBuilder defaults() {
        return new MeasuringClientBuilder();
    }
    
    @Configurable(arg="latency", key="measureLatency", value="false", type=ConfigValueType.BOOLEAN)
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

    @Configurable(arg="throughput", key="measureThroughput", value="true", type=ConfigValueType.BOOLEAN)
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

    protected MeasuringClientBuilder(
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    @Override
    protected MeasuringClientBuilder newInstance(
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return new MeasuringClientBuilder(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
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
                Tracing.getTraceDescription(getRuntimeModule().getConfiguration()), 
                types.build());
    }
    
    @SuppressWarnings({ "unchecked" })
    @Override
    protected TraceEventPublisherService getDefaultTracePublisher() {
        TraceWriter writer = getDefaultTraceWriter();
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
                BusFactory.SynchronousOnly(), 
                actor);
    }
    
    @Override
    protected ConnectionClientExecutorService.Builder getDefaultClientBuilder() {
        return ConnectionClientExecutorService.builder()
                .setConnectionBuilder(
                        ClientConnectionFactoryBuilder.defaults()
                        .setCodecFactory(
                                new Factory<OperationTracingCodec>() {
                                    @Override
                                    public OperationTracingCodec get() {
                                        return OperationTracingCodec.defaults(getTracePublisher().getPublisher());
                                    }
                                }))
                .setRuntimeModule(getRuntimeModule())
                .setDefaults();
    }

    @Override
    protected Generator<? extends Records.Request> getDefaultRequestGenerator() {
        return Generators.constant(
                Operations.Requests.exists().setPath(ZNodePath.root()).setWatch(false).build());
    }
}
