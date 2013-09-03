package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.clients.common.SubmitIterator;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceIteratorClientBuilder extends TraceClientBuilder<TraceIteratorClientBuilder> {

    public static TraceIteratorClientBuilder defaults() {
        return new TraceIteratorClientBuilder();
    }
    
    public class Module extends TraceClientBuilder<TraceIteratorClientBuilder>.Module {

        @Provides @Singleton
        public Iterator<Records.Request> getRequests(
                Configuration configuration,
                ObjectMapper mapper) throws IOException {
            File file = Trace.getTraceInputFileConfiguration(configuration);
            logger.info("Trace input: {}", file);
            Iterator<TraceEvent> events = TraceEventIterator.forFile(file, mapper.reader());
            return TraceRequestIterator.requestsOf(TraceRequestIterator.from(events));
        }
    }

    protected TraceIteratorClientBuilder() {
        this(null, null, null);
    }
    
    protected TraceIteratorClientBuilder(
            Injector injector,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        super(injector, connectionBuilder, clientConnectionFactory);
    }

    @Override
    public TraceIteratorClientBuilder setInjector(Injector injector) {
        return new TraceIteratorClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }

    @Override
    public TraceIteratorClientBuilder setRuntimeModule(RuntimeModule runtime) {
        return new TraceIteratorClientBuilder(injector, connectionBuilder.setRuntimeModule(runtime), clientConnectionFactory);
    }
    
    @Override
    public TraceIteratorClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return new TraceIteratorClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    public TraceIteratorClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        return new TraceIteratorClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    protected Injector getDefaultInjector() {
        return Guice.createInjector(new Module());
    }

    @Override
    protected List<Service> getServices() {
        List<Service> services = super.getServices();
        services.add(RunnableService.create(getRunnable()));
        return services;
    }

    protected Runnable getRunnable() {
        ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> client = getDefaultClientExecutor();
        Iterator<Records.Request> requests = injector.getInstance(Key.get(new TypeLiteral<Iterator<Records.Request>>(){}));
        injector.getInstance(ServiceMonitor.class).add(injector.getInstance(TraceEventPublisherService.class));
        final Iterator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = SubmitIterator.create(requests, client);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> next = null;
                    while (operations.hasNext()) {
                        next = operations.next();
                    }
                    if (next != null) {
                        next.second().get();
                    }
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
