package edu.uw.zookeeper.clients.trace;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.RuntimeModule;

public abstract class Tracing {

    public static JsonEncoding ENCODING = JsonEncoding.UTF8;
    
    public static File getTraceOutputFileConfiguration(Configuration configuration) {
        return TraceOutputFileConfiguration.get(configuration);
    }

    public static File getTraceInputFileConfiguration(Configuration configuration) {
        return TraceInputFileConfiguration.get(configuration);
    }
    
    public static Map<String, Object> getTraceDescription(Configuration configuration) {
        return TraceDescriptionConfiguration.get(configuration);
    }
    
    @Configurable(path="Trace", arg="output", key="OutputFile", value="trace-%d.json", help="Path")
    public static class TraceOutputFileConfiguration implements Function<Configuration, File> {

        public static File get(Configuration configuration) {
            return new TraceOutputFileConfiguration().apply(configuration);
        }

        @Override
        public File apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            String value =
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key());
            if (value.indexOf('%') != -1) {
                value = String.format(value, System.currentTimeMillis());
            }
            return new File(value).getAbsoluteFile();
        }   
    }
    
    @Configurable(path="Trace", arg="input", key="InputFile", value="trace.json", help="Path")
    public static class TraceInputFileConfiguration implements Function<Configuration, File> {

        public static File get(Configuration configuration) {
            return new TraceInputFileConfiguration().apply(configuration);
        }

        @Override
        public File apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            String value =
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key());
            return new File(value).getAbsoluteFile();
        }   
    }
    
    @Configurable(arg="description", path="Trace", key="Description", help="Description", value="{}", type=ConfigValueType.STRING)
    public static class TraceDescriptionConfiguration implements Function<Configuration, Map<String, Object>> {

        public static Map<String, Object> get(Configuration configuration) {
            return new TraceDescriptionConfiguration().apply(configuration);
        }

        @Override
        public Map<String, Object> apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                String description = config.getString(configurable.key());
                try {
                    return new ObjectMapper().readValue(
                            description,
                            new TypeReference<Map<String,Object>>() {});
                } catch (IOException e) {
                    throw new IllegalArgumentException(description, e);
                }
            } else {
                return ImmutableMap.of();
            }
        }
    }
    
    public static abstract class TracingBuilder<T, C extends TracingBuilder<T,C>> implements ZooKeeperApplication.RuntimeBuilder<T, C> {
        
        protected final Logger logger = LogManager.getLogger(getClass());
        protected final ObjectMapper mapper;
        protected final RuntimeModule runtime;
        
        protected TracingBuilder(
                ObjectMapper mapper,
                RuntimeModule runtime) {
            this.mapper = mapper;
            this.runtime = runtime;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setRuntimeModule(RuntimeModule runtime) {
            if (this.runtime == runtime) {
                return (C) this;
            } else {
                return newInstance(mapper, runtime);
            }
        }
        
        public ObjectMapper getObjectMapper() {
            return mapper;
        }
        
        @SuppressWarnings("unchecked")
        public C setObjectMapper(ObjectMapper mapper) {
            if (this.mapper == mapper) {
                return (C) this;
            } else {
                return newInstance(mapper, runtime);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setDefaults() {
            checkState(getRuntimeModule() != null);
        
            if (mapper == null) {
                return setObjectMapper(getDefaultObjectMapper()).setDefaults();
            }
            
            return (C) this;
        }
        
        @Override
        public T build() {
            return setDefaults().doBuild();
        }
        
        protected ObjectMapper getDefaultObjectMapper() {
            return ObjectMapperBuilder.defaults().build();
        }

        protected abstract C newInstance(ObjectMapper mapper, RuntimeModule runtime);
        
        protected abstract T doBuild();
    }
    
    public static abstract class TraceWritingBuilder<T, C extends TraceWritingBuilder<T,C>> extends TracingBuilder<T,C> {

        protected final TraceWriterBuilder writerBuilder;
        protected final TraceEventPublisherService tracePublisher;
        
        protected TraceWritingBuilder(
                TraceWriterBuilder writerBuilder,
                TraceEventPublisherService tracePublisher,
                ObjectMapper mapper,
                RuntimeModule runtime) {
            super(mapper, runtime);
            this.writerBuilder = writerBuilder;
            this.tracePublisher = tracePublisher;
        }

        public TraceWriterBuilder getTraceWriterBuilder() {
            return writerBuilder;
        }

        @SuppressWarnings("unchecked")
        public C setTraceWriterBuilder(
                TraceWriterBuilder writerBuilder) {
            if (this.writerBuilder == writerBuilder) {
                return (C) this;
            } else {
                return newInstance(writerBuilder, tracePublisher, mapper, runtime);
            }
        }

        public TraceEventPublisherService getTracePublisher() {
            return tracePublisher;
        }

        @SuppressWarnings("unchecked")
        public C setTracePublisher(
                TraceEventPublisherService tracePublisher) {
            if (this.tracePublisher == tracePublisher) {
                return (C) this;
            } else {
                return newInstance(writerBuilder, tracePublisher, mapper, runtime);
            }
        }

        @Override
        public C setDefaults() {
            C builder = super.setDefaults();
            if (this == builder) {
                if (writerBuilder == null) {
                    return setTraceWriterBuilder(getDefaultWriterBuilder()).setDefaults();
                }
                if (tracePublisher == null) {
                    return setTracePublisher(getDefaultTracePublisher()).setDefaults();
                }
            }
            return builder;
        }
        
        @Override
        protected C newInstance(
                ObjectMapper mapper,
                RuntimeModule runtime) {
            return newInstance(writerBuilder, tracePublisher, mapper, runtime);
        }
        
        protected abstract C newInstance(
                TraceWriterBuilder writerBuilder,
                TraceEventPublisherService tracePublisher,
                ObjectMapper mapper,
                RuntimeModule runtime);

        protected TraceHeader getDefaultTraceHeader() {
            return TraceHeader.create(
                    Tracing.getTraceDescription(getRuntimeModule().getConfiguration()), 
                    TraceEventTag.TIMESTAMP_EVENT, 
                    TraceEventTag.PROTOCOL_REQUEST_EVENT, 
                    TraceEventTag.PROTOCOL_RESPONSE_EVENT);
        }

        protected TraceWriterBuilder getDefaultWriterBuilder() {
            ObjectWriter writer = mapper.writer();
            File file = Tracing.getTraceOutputFileConfiguration(getRuntimeModule().getConfiguration());
            logger.info("Trace output: {}", file);
            Executor executor = getRuntimeModule().getExecutors().get(ExecutorService.class);
            TraceHeader header = getDefaultTraceHeader();
            return TraceWriterBuilder.defaults()
                    .setHeader(header)
                    .setWriter(writer)
                    .setExecutor(executor)
                    .setFile(file);
        }
        
        protected TraceEventPublisherService getDefaultTracePublisher() {
            return TraceEventPublisherService.newInstance(
                    EventBusPublisher.newInstance(), 
                    writerBuilder.build());
        }
    }
}
