package edu.uw.zookeeper.clients.trace;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.internal.ImmutableSet;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;

public class MeasuringClientModule extends TraceWriterClientModule {

    public static ParameterizedFactory<RuntimeModule, Application> factory() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                try {
                    return new MeasuringClientModule(runtime).call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }  
        };
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
    
    public class Module extends TraceWriterClientModule.Module {

        @Override
        protected void configure() {
            install(JacksonModule.create());
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
    
    protected MeasuringClientModule(RuntimeModule runtime) {
        super(runtime);
    }
    
    @Override
    protected com.google.inject.Module module() {
        return new Module();
    }

    @Override
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return OperationTracingCodec.factory(injector.getInstance(Publisher.class));
    }

    @Override
    protected TraceHeader getTraceHeader(Configuration configuration) {
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
                TraceDescriptionConfiguration.get(configuration), 
                types.build());
    }
}
