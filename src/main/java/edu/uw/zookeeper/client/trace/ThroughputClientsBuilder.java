package edu.uw.zookeeper.client.trace;

import java.util.List;
import java.util.Set;

import net.engio.mbassy.bus.BusFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.ConnectionClientExecutorsService;
import edu.uw.zookeeper.client.IteratingClient;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Generators;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RunnableService;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.Operations;
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
    
    @SuppressWarnings("unchecked")
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
                BusFactory.SynchronousOnly(), 
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
    
    protected Generator<? extends Records.Request> getDefaultRequestGenerator() {
        return Generators.constant(
                Operations.Requests.exists().setPath(ZNodePath.root()).setWatch(false).build());
    }
    
    protected Runnable getDefaultRunnable() {
        final int nclients = ConfigurableClients.get(getRuntimeModule().getConfiguration());
        final int outstanding = LimitOutstandingClient.ConfigurableLimit.get(getRuntimeModule().getConfiguration());
        final int iterations = CountingGenerator.ConfigurableIterations.get(getRuntimeModule().getConfiguration());
        final int logInterval = 0;
        final Generator<? extends Records.Request> generator = getDefaultRequestGenerator();
        final ListeningExecutorService executor = getRuntimeModule().getExecutors().get(ListeningExecutorService.class);
        final Logger logger = LogManager.getLogger(IteratingClient.class);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    List<ConnectionClientExecutor<Operation.Request,?,?,?>> executors = Lists.newArrayListWithCapacity(nclients);
                    for (int i=0; i<nclients; ++i) {
                        executors.add(connectionBuilder.getConnectionClientExecutors().get().get());
                    }
                    
                    List<IteratingClient> clients = Lists.newArrayListWithCapacity(executors.size());
                    for (ConnectionClientExecutor<Operation.Request, ?, ?, ?> e: executors) {
                        CountingGenerator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> task = 
                                CountingGenerator.create(
                                iterations, 
                                logInterval,
                                SubmitGenerator.create(
                                        generator, 
                                        LimitOutstandingClient.create(outstanding, e)),
                                logger);
                        IteratingClient client = IteratingClient.create(
                                executor, 
                                task, 
                                SettableFuturePromise.<Void>create());
                        LoggingFutureListener.listen(logger, client);
                        clients.add(client);
                    }
                    
                    List<ListenableFuture<?>> futures = Lists.newArrayListWithCapacity(clients.size());
                    for (IteratingClient e: clients) {
                        futures.add(e);
                        executor.execute(e);
                    }
                    
                    Futures.allAsList(futures).get();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
