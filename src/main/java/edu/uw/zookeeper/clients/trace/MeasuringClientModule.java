package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.inject.Provides;
import com.google.inject.Singleton;
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

public class MeasuringClientModule extends TraceGeneratorClientModule {

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
    
    public static class Module extends TraceGeneratorClientModule.Module {

        public static Module create() {
            return new Module();
        }

        @Override
        protected void configure() {
            install(JacksonModule.create());
        }
        
        @Provides @Singleton
        public Actor<TraceEvent> getTraceEventActor(
                Configuration configuration,
                TraceWriter writer) {
            boolean measureLatency = MeasureLatencyConfiguration.get(configuration);
            boolean measureThroughput = MeasureThroughputConfiguration.get(configuration);
            Predicate<TraceEvent> filter = new Predicate<TraceEvent>() {
                @Override
                public boolean apply(TraceEvent input) {
                    switch (input.getTag()) {
                    case TIMESTAMP_EVENT:
                    case LATENCY_MEASUREMENT_EVENT:
                    case THROUGHPUT_MEASUREMENT_EVENT:
                        return true;
                    default:
                        return false;
                    }
                }
            };
            
            Actor<TraceEvent> actor = FilteringTraceEventActor.create(
                    filter, writer);
            if (measureLatency) {
                actor = LatencyMeasuringActor.create(actor);
            }
            if (measureThroughput) {
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
        return Module.create();
    }

    @Override
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return OperationTracingCodec.factory(injector.getInstance(Publisher.class));
    }
}
