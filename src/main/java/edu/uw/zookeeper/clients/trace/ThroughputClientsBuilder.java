package edu.uw.zookeeper.clients.trace;

import java.util.List;
import java.util.Set;

import net.engio.mbassy.bus.config.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.clients.ConnectionClientExecutorsService;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.Generators;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.clients.random.PathedRequestGenerator;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.proto.Records;

public class ThroughputClientsBuilder extends Tracing.TraceWritingBuilder<List<Service>, ThroughputClientsBuilder> {

    public static ThroughputClientsBuilder defaults() {
        return new ThroughputClientsBuilder(
                null, null, null, null, null);
    }
    
    @Configurable(arg="clients", key="clients", value="100", type=ConfigValueType.NUMBER)
    public static class ConfigurableClients implements Function<Configuration, Integer> {

        public static Integer get(Configuration configuration) {
            return new ConfigurableClients().apply(configuration);
        }

        @Override
        public Integer apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getInt(configurable.key());
        }   
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final ConnectionClientExecutorsService.OperationBuilder connectionBuilder;
    
    protected ThroughputClientsBuilder(
            ConnectionClientExecutorsService.OperationBuilder connectionBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher, 
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(writerBuilder, tracePublisher, mapper, runtime);
        this.connectionBuilder = connectionBuilder;
    }

    public ConnectionClientExecutorsService.OperationBuilder getConnectionBuilder() {
        return connectionBuilder;
    }

    public ThroughputClientsBuilder setConnectionBuilder(ConnectionClientExecutorsService.OperationBuilder connectionBuilder) {
        if (this.connectionBuilder == connectionBuilder) {
            return this;
        } else {
            return newInstance(connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
        }
    }

    @Override
    public ThroughputClientsBuilder setDefaults() {
        ThroughputClientsBuilder builder = super.setDefaults();
        if (this == builder) {
            if (connectionBuilder == null) {
                return setConnectionBuilder(getDefaultConnectionBuilder()).setDefaults();
            }
            ConnectionClientExecutorsService.OperationBuilder connectionBuilder = this.connectionBuilder.setDefaults();
            if (this.connectionBuilder != connectionBuilder) {
                return setConnectionBuilder(connectionBuilder).setDefaults();
            }
        }
        return builder;
    }

    @Override
    protected ThroughputClientsBuilder newInstance(
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher, 
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return newInstance(connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    protected ThroughputClientsBuilder newInstance(
            ConnectionClientExecutorsService.OperationBuilder connectionBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher, 
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return new ThroughputClientsBuilder(connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    @Override
    protected List<Service> doBuild() {
        List<Service> services = Lists.newArrayList();
        services.add(getTracePublisher());
        services.addAll(getConnectionBuilder().build());
        services.add(RunnableService.create(getDefaultRunnable()));
        return services;
    }

    @Override
    protected TraceHeader getDefaultTraceHeader() {
        return TraceHeader.create(
                Tracing.getTraceDescription(getRuntimeModule().getConfiguration()), 
                TraceEventTag.TIMESTAMP_EVENT, 
                TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    protected TraceEventPublisherService getDefaultTracePublisher() {
        TraceWriter writer = getDefaultTraceWriter();
        final Set<TraceEventTag> types = writer.header().getTypes();
        Predicate<TraceEvent> filter = new Predicate<TraceEvent>() {
            @Override
            public boolean apply(TraceEvent input) {
                return types.contains(input.getTag());
            }
        };
        Actor<TraceEvent> actor = ThroughputMeasuringActor.create(
                    getRuntimeModule().getConfiguration(),
                    FilteringTraceEventActor.create(
                            filter, writer));
        return TraceEventPublisherService.newInstance(
                new SyncMessageBus<Object>(new SyncBusConfiguration()), 
                actor);
    }
    
    protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
        return ClientConnectionFactoryBuilder.defaults()
                .setCodecFactory(
                        new Factory<OperationTracingCodec>() {
                            @Override
                            public OperationTracingCodec get() {
                                return OperationTracingCodec.defaults(getTracePublisher().getPublisher());
                            }
                        })
                .setRuntimeModule(getRuntimeModule())
                .setDefaults();
    }
    
    protected ConnectionClientExecutorsService.OperationBuilder getDefaultConnectionBuilder() {
        return ConnectionClientExecutorsService.builder()
                .setConnectionBuilder(getDefaultClientConnectionFactoryBuilder())
                .setRuntimeModule(getRuntimeModule())
                .setDefaults(); 
    }
    

    protected Generator<Records.Request> getDefaultRequestGenerator() {
        return PathedRequestGenerator.exists(Generators.constant(ZNodePath.root()));
    }
    
    protected Runnable getDefaultRunnable() {
        final int nclients = ConfigurableClients.get(getRuntimeModule().getConfiguration());
        final int outstanding = LimitOutstandingClient.ConfigurableLimit.get(getRuntimeModule().getConfiguration());
        final int iterations = IterationCallable.ConfigurableIterations.get(getRuntimeModule().getConfiguration());
        final int logInterval = 0;
        final Generator<Records.Request> generator = getDefaultRequestGenerator();
        final ListeningExecutorService executor = getRuntimeModule().getExecutors().get(ListeningExecutorService.class);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    List<ConnectionClientExecutor<Operation.Request,?,?,?>> executors = Lists.newArrayListWithCapacity(nclients);
                    for (int i=0; i<nclients; ++i) {
                        executors.add(connectionBuilder.getConnectionClientExecutors().get().get());
                    }
                    
                    List<Client> clients = Lists.newArrayListWithCapacity(executors.size());
                    for (ConnectionClientExecutor<Operation.Request,?,?,?> e: executors) {
                        IterationCallable<? extends Pair<Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> task = IterationCallable.create(
                                iterations, logInterval,
                                SubmitCallable.create(
                                        generator, 
                                        LimitOutstandingClient.create(outstanding, e)));
                        Client client = new Client(
                                executor, 
                                task, 
                                LoggingPromise.create(logger, SettableFuturePromise.<Void>create()));
                        clients.add(client);
                    }
                    
                    List<ListenableFuture<?>> futures = Lists.newArrayListWithCapacity(clients.size());
                    for (Client e: clients) {
                        e.run();
                        futures.add(e);
                    }
                    
                    Futures.allAsList(futures).get();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
    
    protected static class Client extends PromiseTask<IterationCallable<? extends Pair<Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Void> implements Runnable, FutureCallback<Object> {
        
        protected final ListeningExecutorService executor;
        
        public Client(
                ListeningExecutorService executor,
                IterationCallable<? extends Pair<Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> task,
                Promise<Void> promise) {
            super(task, promise);
            this.executor = executor;
        }
        
        @Override
        public void run() {
            if (! isDone()) {
                Futures.addCallback(executor.submit(task()), this, executor);
            }
        }

        @Override
        public void onSuccess(Object result) {
            if (result instanceof Optional) {
                if (((Optional<?>) result).isPresent()) {
                    Futures.addCallback((ListenableFuture<?>) ((Pair<?,?>) ((Optional<?>) result).get()).second(), this, executor);
                } else {
                    run();
                }
            } else {
                set(null);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}
