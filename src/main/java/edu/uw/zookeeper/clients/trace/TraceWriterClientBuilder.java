package edu.uw.zookeeper.clients.trace;

import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.clients.common.CallUntilPresent;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.clients.random.PathedRequestGenerator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class TraceWriterClientBuilder<C extends TraceWriterClientBuilder<C>> extends TraceClientBuilder<C> {

    public class Module extends TraceClientBuilder<C>.Module {

        protected Module() {
            super();
        }

        @Provides @Singleton
        public ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> getCache(
                ClientConnectionExecutorService client,
                ServiceMonitor monitor) {
            return builder.getDefaultCache();
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

    protected TraceWriterClientBuilder(
            Injector injector,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionExecutorService clientExecutor) {
        super(injector, connectionBuilder, clientConnectionFactory, clientExecutor);
    }

    public ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> getCache() {
        return injector.getInstance(Key.get(new TypeLiteral<ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>>>(){}));
    }

    @Override
    protected List<Service> getServices() {
        List<Service> services = super.getServices();
        services.add(injector.getInstance(FetchCacheService.class));
        services.add(RunnableService.create(getRunnable()));
        return services;
    }
    
    protected Runnable getRunnable() {
        final CallUntilPresent<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                    CallUntilPresent.create(
                        IterationCallable.create(getRuntimeModule().configuration(), 
                                SubmitCallable.create(getDefaultRequestGenerator(), getDefaultClientExecutor())));
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

    protected ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> getDefaultCache() {
        return ZNodeViewCache.newInstance(clientExecutor, clientExecutor);
    }

    @Override
    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> getDefaultClientExecutor() {
        return LimitOutstandingClient.create(
                getRuntimeModule().configuration(), 
                getCache());
    }

    protected Generator<Records.Request> getDefaultRequestGenerator() {
        return PathedRequestGenerator.create(getCache());
    }
}
