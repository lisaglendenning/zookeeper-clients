package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.clients.CallUntilPresent;
import edu.uw.zookeeper.clients.Generator;
import edu.uw.zookeeper.clients.IterationCallable;
import edu.uw.zookeeper.clients.PathedRequestGenerator;
import edu.uw.zookeeper.clients.RunnableService;
import edu.uw.zookeeper.clients.SubmitCallable;
import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceGeneratorClientModule extends ClientApplicationModule {

    public static ParameterizedFactory<RuntimeModule, Application> factory() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                try {
                    return new TraceGeneratorClientModule(runtime).call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }  
        };
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        @Override
        protected void configure() {
            install(JacksonModule.create());
        }
        
        @Provides @Singleton
        public TraceWriter getTraceWriter(
                ObjectMapper mapper,
                Configuration configuration,
                Executor executor) throws IOException {
            String traceFile = Trace.getTraceFileConfiguration(configuration);
            return TraceWriter.forFile(
                    new File(traceFile), 
                    mapper.writer(), 
                    executor);
        }
        
        @Provides @Singleton
        public Publisher getTracePublisher(
                Factory<? extends Publisher> publishers) {
            return publishers.get();
        }
        
        @Provides @Singleton
        public TraceEventWriterService getTraceEventWriterService(
                TraceWriter writer, Publisher publisher) {
            return TraceEventWriterService.newInstance(publisher, writer);
        }
        
        @Provides @Singleton
        public IntervalTimestampGenerator getIntervalTimestampGenerator(
                Configuration configuration, Publisher publisher) {
            return IntervalTimestampGenerator.create(publisher, configuration);
        }
    }
    
    protected final Injector injector;

    protected TraceGeneratorClientModule(RuntimeModule runtime) {
        super(runtime);
        this.injector = createInjector(runtime);
    }
    
    protected Injector createInjector(RuntimeModule runtime) {
        return Guice.createInjector(
                RuntimeModuleProvider.create(runtime), 
                new Module());
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

    protected Generator<Records.Request> getRequestGenerator(ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache) {
        return PathedRequestGenerator.create(cache);
    }

    protected Runnable getRunnable() {
        ClientConnectionExecutorService<PingingClient<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> client = getClientConnectionExecutorService(getTimeOut());
        runtime.serviceMonitor().add(injector.getInstance(TraceEventWriterService.class));
        runtime.serviceMonitor().add(injector.getInstance(IntervalTimestampGenerator.class));
        final ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = ZNodeViewCache.newInstance(client, client);
        final Generator<Records.Request> requests = getRequestGenerator(cache);
        final ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> limiting = LimitOutstandingClient.create(runtime.configuration(), cache);
        final CallUntilPresent<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                    CallUntilPresent.create(
                        IterationCallable.create(runtime.configuration(), 
                                SubmitCallable.create(requests, limiting)));
        return new Runnable() {
            @Override
            public void run() {
                try {
                    callable.call().second().get();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
