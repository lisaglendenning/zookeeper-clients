package edu.uw.zookeeper.client.trace;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.client.random.RandomRequestGenerator;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ServerResponse;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceGeneratingCacheClientBuilder extends TraceGeneratingClientBuilder<TraceGeneratingCacheClientBuilder> {

    public static TraceGeneratingCacheClientBuilder defaults() {
        return new TraceGeneratingCacheClientBuilder();
    }

    protected static class FetchCacheService extends AbstractIdleService {

        private final LockableZNodeCache<?, Operation.Request, Message.ServerResponse<?>> cache;
        
        public FetchCacheService(LockableZNodeCache<?, Operation.Request, Message.ServerResponse<?>> cache) {
            this.cache = checkNotNull(cache);
        }
        
        @Override
        protected void startUp() throws Exception {
            TreeFetcher.builder().setClient(cache).build().get();
        }

        @Override
        protected void shutDown() throws Exception {
        }
    }
    
    protected final LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, Message.ServerResponse<?>> cache;

    protected TraceGeneratingCacheClientBuilder() {
        this(null, null, null, null, null, null);
    }
    
    protected TraceGeneratingCacheClientBuilder(
            LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, Message.ServerResponse<?>> cache,
                    ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
        this.cache = cache;
    }
    
    public LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, Message.ServerResponse<?>> getCache() {
        return cache;
    }

    public TraceGeneratingCacheClientBuilder setCache(
            LockableZNodeCache<ZNodeCache.SimpleCacheNode, Request, ServerResponse<?>> cache) {
        return newInstance(cache, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    @Override
    public TraceGeneratingCacheClientBuilder setDefaults() {
        TraceGeneratingCacheClientBuilder builder = super.setDefaults();
        if (builder == this) {
            if (cache == null) {
                return setCache(getDefaultCache()).setDefaults();
            }
        }
        return builder;
    }
    
    @Override
    protected TraceGeneratingCacheClientBuilder newInstance(
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return newInstance(cache, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    protected TraceGeneratingCacheClientBuilder newInstance(
            LockableZNodeCache<ZNodeCache.SimpleCacheNode, Request, ServerResponse<?>> cache,
            ConnectionClientExecutorService.Builder clientBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,  
            ObjectMapper mapper, 
            RuntimeModule runtime) {
        return new TraceGeneratingCacheClientBuilder(cache, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    @Override
    protected List<Service> doBuild() {
        List<Service> services = super.doBuild();
        services.add(services.size() - 1, new FetchCacheService(getCache()));
        return services;
    }

    protected LockableZNodeCache<ZNodeCache.SimpleCacheNode, Operation.Request, Message.ServerResponse<?>> getDefaultCache() {
        return LockableZNodeCache.newInstance(
                getClientBuilder().getConnectionClientExecutor());
    }

    @Override
    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>, ?> getDefaultClientExecutor() {
        return LimitOutstandingClient.create(
                getRuntimeModule().getConfiguration(), 
                getCache());
    }

    @Override
    protected Generator<Records.Request> getDefaultRequestGenerator() {
        return RandomRequestGenerator.fromCache(getCache());
    }
}
