package edu.uw.zookeeper.clients.trace;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

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

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.ClientBuilder.ConfigurableEnsembleView;
import edu.uw.zookeeper.clients.ClientConnectionExecutorsService;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.clients.random.ConstantGenerator;
import edu.uw.zookeeper.clients.random.PathedRequestGenerator;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.Records;

public class ThroughputClientsBuilder extends Tracing.TraceWritingBuilder<List<Service>, ThroughputClientsBuilder> {

    public static ThroughputClientsBuilder defaults() {
        return new ThroughputClientsBuilder(null, null, null, null, null, null, null);
    }
    
    @Configurable(arg="clients", key="Clients", value="100", type=ConfigValueType.NUMBER)
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
    protected final ClientConnectionExecutorsService<?> clientExecutors;
    protected final ClientConnectionFactoryBuilder connectionBuilder;
    protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory;
    
    protected ThroughputClientsBuilder(
            ClientConnectionExecutorsService<?> clientExecutors,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionFactoryBuilder connectionBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher, 
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(writerBuilder, tracePublisher, mapper, runtime);
        this.clientExecutors = clientExecutors;
        this.clientConnectionFactory = clientConnectionFactory;
        this.connectionBuilder = connectionBuilder;
    }

    public ClientConnectionFactoryBuilder getConnectionBuilder() {
        return connectionBuilder;
    }

    public ThroughputClientsBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        if (this.connectionBuilder == connectionBuilder) {
            return this;
        } else {
            return newInstance(clientExecutors, clientConnectionFactory, connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
        }
    }

    public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClientConnectionFactory() {
        return clientConnectionFactory;
    }

    public ThroughputClientsBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        if (this.clientConnectionFactory == clientConnectionFactory) {
            return this;
        } else {
            return newInstance(clientExecutors, clientConnectionFactory, connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
        }
    }
    
    public ClientConnectionExecutorsService<?> getClientConnectionExecutors() {
        return clientExecutors;
    }

    public ThroughputClientsBuilder setClientConnectionExecutors(
            ClientConnectionExecutorsService<?> clientExecutors) {
        if (this.clientExecutors == clientExecutors) {
            return this;
        } else {
            return newInstance(clientExecutors, clientConnectionFactory, connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
        }
    }

    @Override
    public ThroughputClientsBuilder setDefaults() {
        ThroughputClientsBuilder builder = super.setDefaults();
        if (this == builder) {
            if (connectionBuilder == null) {
                return setConnectionBuilder(getDefaultClientConnectionFactoryBuilder()).setDefaults();
            }
            if (clientConnectionFactory == null) {
                return setClientConnectionFactory(getDefaultClientConnectionFactory()).setDefaults();
            }
            if (clientExecutors == null) {
                return setClientConnectionExecutors(getDefaultClientConnectionExecutorsService()).setDefaults();
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
        return newInstance(clientExecutors, clientConnectionFactory, connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    protected ThroughputClientsBuilder newInstance(
            ClientConnectionExecutorsService<?> clientExecutors,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
            ClientConnectionFactoryBuilder connectionBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher, 
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return new ThroughputClientsBuilder(clientExecutors, clientConnectionFactory, connectionBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    @Override
    protected List<Service> doBuild() {
        List<Service> services = Lists.newArrayList(
                tracePublisher,
                clientConnectionFactory,
                clientExecutors,
                RunnableService.create(getDefaultRunnable()));
        return services;
    }

    @Override
    protected TraceHeader getDefaultTraceHeader() {
        return TraceHeader.create(
                Tracing.getTraceDescription(getRuntimeModule().getConfiguration()), 
                TraceEventTag.TIMESTAMP_EVENT, 
                TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT);
    }
    
    @Override
    protected TraceEventPublisherService getDefaultTracePublisher() {
        TraceWriter writer = writerBuilder.build();
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
                EventBusPublisher.newInstance(), 
                actor);
    }
    
    protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
        return ClientConnectionFactoryBuilder.defaults()
                .setCodecFactory(OperationTracingCodec.factory(tracePublisher.getPublisher()))
                .setRuntimeModule(runtime)
                .setDefaults();
    }
    
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
        return connectionBuilder.build();
    }
    
    protected ClientConnectionExecutorsService<?> getDefaultClientConnectionExecutorsService() {
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
        final EnsembleViewFactory<? extends ServerViewFactory<Session, ?>> ensembleFactory = 
                EnsembleViewFactory.fromSession(
                    clientConnectionFactory,
                    ensemble, 
                    connectionBuilder.getTimeOut(),
                    getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
        ClientConnectionExecutorsService<?> service =
                ClientConnectionExecutorsService.newInstance(
                        new Factory<ListenableFuture<? extends ClientConnectionExecutor<?>>>() {
                            @Override
                            public ListenableFuture<? extends ClientConnectionExecutor<?>> get() {
                                return ensembleFactory.get().get();
                            }
                        });
        return service;
    }

    protected Generator<Records.Request> getDefaultRequestGenerator() {
        return PathedRequestGenerator.exists(ConstantGenerator.of(ZNodeLabel.Path.root()));
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
                    List<ClientConnectionExecutor<?>> executors = Lists.newArrayListWithCapacity(nclients);
                    for (int i=0; i<nclients; ++i) {
                        executors.add(clientExecutors.get().get());
                    }
                    
                    List<Client> clients = Lists.newArrayListWithCapacity(executors.size());
                    for (ClientConnectionExecutor<?> e: executors) {
                        IterationCallable<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> task = IterationCallable.create(
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
    
    protected static class Client extends PromiseTask<IterationCallable<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>>, Void> implements Runnable, FutureCallback<Object> {
        
        protected final ListeningExecutorService executor;
        
        public Client(
                ListeningExecutorService executor,
                IterationCallable<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> task,
                Promise<Void> promise) {
            super(task, promise);
            this.executor = executor;
        }
        
        @Override
        public void run() {
            Futures.addCallback(executor.submit(task()), this, executor);
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
