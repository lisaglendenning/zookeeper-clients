package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.clients.CallUntilPresent;
import edu.uw.zookeeper.clients.Generator;
import edu.uw.zookeeper.clients.IterationCallable;
import edu.uw.zookeeper.clients.RunnableService;
import edu.uw.zookeeper.clients.SubmitCallable;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class TraceWriterClientModule extends TraceClientModule {

    public class Module extends TraceClientModule.Module {

        @Override
        protected void configure() {
            super.configure();
            bind(new TypeLiteral<Actor<TraceEvent>>() {}).to(TraceWriter.class);
        }
        
        @Provides @Singleton
        public TraceWriter getTraceWriter(
                File traceFile,
                ObjectMapper mapper,
                Executor executor) throws IOException {
            return TraceWriter.forFile(
                    traceFile, 
                    mapper.writer(), 
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
            return TraceWriterClientModule.this.getClientConnectionExecutorService(getTimeOut());
        }
        
        @Provides @Singleton
        public ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> getCache(
                ClientConnectionExecutorService<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> client,
                        ServiceMonitor monitor) {
            ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = ZNodeViewCache.newInstance(client, client);
            monitor.add(new FetchCacheService(cache));
            return cache;
        }
    }
    
    @Singleton
    protected static class FetchCacheService extends AbstractIdleService {

        private final ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache;
        
        @Inject
        public FetchCacheService(ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache) {
            this.cache = cache;
        }
        
        @Override
        protected void startUp() throws Exception {
            TreeFetcher.builder().setClient(cache).build().apply(ZNodeLabel.Path.root()).get();
        }

        @Override
        protected void shutDown() throws Exception {
        }
    }
    
    protected TraceWriterClientModule(RuntimeModule runtime) {
        super(runtime);
    }

    @Override
    protected com.google.inject.Module module() {
        return new Module();
    }

    @Override
    protected ServiceMonitor createServices() {
        runtime.serviceMonitor().add(
                RunnableService.create(getRunnable()));
        return runtime.serviceMonitor();
    }
    
    protected Runnable getRunnable() {
        final CallUntilPresent<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                    CallUntilPresent.create(
                        IterationCallable.create(runtime.configuration(), 
                                SubmitCallable.create(getRequestGenerator(), getClient())));
        injector.getInstance(ServiceMonitor.class).add(injector.getInstance(TraceEventPublisherService.class));
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

    protected ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> getCache() {
        return injector.getInstance(Key.get(new TypeLiteral<ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>>>() {}));
    }

    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> getClient() {
        return LimitOutstandingClient.create(
                runtime.configuration(), 
                getCache());
    }
    
    protected abstract Generator<Records.Request> getRequestGenerator();
}
