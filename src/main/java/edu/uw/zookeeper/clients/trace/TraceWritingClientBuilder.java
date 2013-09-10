package edu.uw.zookeeper.clients.trace;


import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;

public abstract class TraceWritingClientBuilder<C extends TraceWritingClientBuilder<C>> extends Tracing.TraceWritingBuilder<List<Service>, C> {

    protected final ClientBuilder clientBuilder;

    protected TraceWritingClientBuilder(
            ClientBuilder clientBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(writerBuilder, tracePublisher, mapper, runtime);
        this.clientBuilder = clientBuilder;
    }
    
    public ClientBuilder getClientBuilder() {
        return clientBuilder;
    }

    @SuppressWarnings("unchecked")
    public C setClientBuilder(ClientBuilder clientBuilder) {
        if (this.clientBuilder == clientBuilder) {
            return (C) this;
        } else {
            return newInstance(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
        }
    }

    @Override
    public C setDefaults() {
        C builder = super.setDefaults();
        if (this == builder) {
            if (clientBuilder == null) {
                return setClientBuilder(getDefaultClientBuilder()).setDefaults();
            }
        }
        return builder;
    }

    @Override
    protected C newInstance(
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return newInstance(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    protected abstract C newInstance(
            ClientBuilder clientBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime);

    @Override
    protected List<Service> doBuild() {
        List<Service> services = Lists.newLinkedList();
        services.add(tracePublisher);
        services.addAll(clientBuilder.build());
        services.add(RunnableService.create(getDefaultRunnable()));
        return services;
    }
    
    protected ClientBuilder getDefaultClientBuilder() {
        return ClientBuilder.defaults()
                .setConnectionBuilder(ClientConnectionFactoryBuilder.defaults()
                        .setCodecFactory(ProtocolTracingCodec.factory(tracePublisher.getPublisher())))
                .setRuntimeModule(getRuntimeModule())
                .setDefaults();
    }
    
    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> getDefaultClientExecutor() {
        return LimitOutstandingClient.create(
                getRuntimeModule().getConfiguration(), 
                clientBuilder.getClientConnectionExecutor());
    }
    
    protected abstract Runnable getDefaultRunnable();
}
