package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

public abstract class TraceClientModule extends ClientApplicationModule {

    public class Module extends AbstractModule {

        @Override
        protected void configure() {
            install(JacksonModule.create());
            bind(new TypeLiteral<Actor<TraceEvent>>() {}).to(TraceWriter.class);
        }

        @Provides @Singleton
        public TraceHeader getTraceHeader(Configuration configuration) {
            return TraceClientModule.this.getTraceHeader(configuration);
        }
        
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
        public Publisher getTracePublisher(
                Factory<? extends Publisher> publishers) {
            return publishers.get();
        }
        
        @Provides @Singleton
        public TraceEventPublisherService getTraceEventWriterService(
                Actor<TraceEvent> writer, Publisher publisher) {
            return TraceEventPublisherService.newInstance(publisher, writer);
        }
        
        @Provides @Singleton
        public ClientConnectionExecutorService<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClientConnectionExecutorService() {
            return TraceClientModule.this.getClientConnectionExecutorService(getTimeOut());
        }
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
    
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Injector injector;

    protected TraceClientModule(RuntimeModule runtime) {
        super(runtime);
        this.injector = createInjector(runtime);
    }
    
    public Injector getInjector() {
        return injector;
    }
    
    protected Injector createInjector(RuntimeModule runtime) {
        return Guice.createInjector(
                RuntimeModuleProvider.create(runtime), 
                module());
    }
    
    protected com.google.inject.Module module() {
        return new Module();
    }

    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> getClient() {
        return LimitOutstandingClient.create(
                runtime.configuration(), 
                injector.getInstance(Key.get(new TypeLiteral<ClientConnectionExecutorService<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>>>() {})));
    }
    
    @Override
    protected ServiceMonitor createServices() {
        runtime.serviceMonitor().add(
                RunnableService.create(getRunnable()));
        return runtime.serviceMonitor();
    }
    
    @Override
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return ProtocolTracingCodec.factory(injector.getInstance(Publisher.class));
    }
    
    protected TraceHeader getTraceHeader(Configuration configuration) {
        return TraceHeader.create(
                TraceDescriptionConfiguration.get(configuration), 
                TraceEventTag.TIMESTAMP_EVENT, 
                TraceEventTag.PROTOCOL_REQUEST_EVENT, 
                TraceEventTag.PROTOCOL_RESPONSE_EVENT);
    }
    
    protected abstract Runnable getRunnable();
}
