package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.internal.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.clients.common.DependentModule;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Publisher;

public class TraceModule extends DependentModule {

    public static TraceModule create() {
        return new TraceModule();
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
    
    @Provides @Singleton
    public TraceWriter getTraceWriter(
            Configuration configuration,
            ObjectMapper mapper,
            TraceHeader header,
            Executor executor) throws IOException {
        File file = Trace.getTraceOutputFileConfiguration(configuration);
        logger.info("Trace output: {}", file);
        return TraceWriter.forFile(
                file, 
                mapper.writer(),
                header,
                executor);
    }

    @Provides @Singleton
    public Publisher getTracePublisher() {
        return EventBusPublisher.newInstance();
    }
    
    @Provides @Singleton
    public TraceEventPublisherService getTraceEventWriterService(
            Actor<TraceEvent> writer, Publisher publisher) {
        return TraceEventPublisherService.newInstance(publisher, writer);
    }
    
    @Override
    protected List<Module> getDependentModules() {
        return Lists.<Module>newArrayList(JacksonModule.create());
    }

    @Configurable(arg="description", path="Trace", key="Description", help="Description", type=ConfigValueType.STRING)
    public static class TraceDescriptionConfiguration implements Function<Configuration, String> {

        public static String get(Configuration configuration) {
            return new TraceDescriptionConfiguration().apply(configuration);
        }

        @Override
        public String apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                return config.getString(configurable.key());
            } else {
                return "";
            }
        }
    }
}
