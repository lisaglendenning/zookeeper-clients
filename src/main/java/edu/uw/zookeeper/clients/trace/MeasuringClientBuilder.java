package edu.uw.zookeeper.clients.trace;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.internal.ImmutableSet;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.clients.common.InjectorServiceLocator;
import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;

public class MeasuringClientBuilder extends TraceWriterClientBuilder<MeasuringClientBuilder> {

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
    
    public class Module extends TraceWriterClientBuilder<MeasuringClientBuilder>.Module {

        protected Module() {
            super();
        }

        @Override
        protected void configure() {
            installDependentModules();
            bind(ServiceLocator.class).to(InjectorServiceLocator.class).in(Singleton.class);
        }

        @Provides @Singleton
        public Actor<TraceEvent> getTraceEventActor(
                Configuration configuration,
                TraceWriter writer) {
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
                actor = ThroughputMeasuringActor.create(configuration, actor);
            }
            return actor;
        }
    }
    
    protected MeasuringClientBuilder() {
        this(null, null, null);
    }

    protected MeasuringClientBuilder(
            Injector injector,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        super(injector, connectionBuilder, clientConnectionFactory);
    }

    @Override
    public MeasuringClientBuilder setInjector(Injector injector) {
        return new MeasuringClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    public MeasuringClientBuilder setRuntimeModule(RuntimeModule runtime) {
        return new MeasuringClientBuilder(injector, connectionBuilder.setRuntimeModule(runtime), clientConnectionFactory);
    }
    
    @Override
    public MeasuringClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return new MeasuringClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    public MeasuringClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        return new MeasuringClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    protected Injector getDefaultInjector() {
        return Guice.createInjector(new Module());
    }

    @Override
    protected TraceHeader getDefaultTraceHeader() {
        Configuration configuration = getRuntimeModule().configuration();
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
                TraceModule.TraceDescriptionConfiguration.get(configuration), 
                types.build());
    }

    @Override
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
        return connectionBuilder.setCodecFactory(
                OperationTracingCodec.factory(injector.getInstance(Publisher.class))).build();
    }
}
