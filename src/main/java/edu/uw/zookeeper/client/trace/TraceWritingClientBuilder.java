package edu.uw.zookeeper.client.trace;


import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.RunnableService;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;

public abstract class TraceWritingClientBuilder<C extends TraceWritingClientBuilder<C>> extends Tracing.TraceWritingBuilder<List<Service>, C> {

    protected final ConnectionClientExecutorService.Builder clientBuilder;

    protected TraceWritingClientBuilder(
            ConnectionClientExecutorService.Builder clientBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(writerBuilder, tracePublisher, mapper, runtime);
        this.clientBuilder = clientBuilder;
    }
    
    public ConnectionClientExecutorService.Builder getClientBuilder() {
        return clientBuilder;
    }

    @SuppressWarnings("unchecked")
    public C setClientBuilder(ConnectionClientExecutorService.Builder clientBuilder) {
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
            if (getClientBuilder() == null) {
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
            ConnectionClientExecutorService.Builder clientBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime);

    @Override
    protected List<Service> doBuild() {
        List<Service> services = Lists.newLinkedList();
        services.add(getTracePublisher());
        services.addAll(getClientBuilder().build());
        services.add(RunnableService.create(getDefaultRunnable()));
        return services;
    }
    
    protected ConnectionClientExecutorService.Builder getDefaultClientBuilder() {
        return ConnectionClientExecutorService.builder()
                .setConnectionBuilder(
                        ClientConnectionFactoryBuilder.defaults()
                        .setCodecFactory(
                                new Factory<ProtocolTracingCodec>() {
                                    @Override
                                    public ProtocolTracingCodec get() {
                                        return ProtocolTracingCodec.defaults(getTracePublisher().getPublisher());
                                    }
                                }))
                .setRuntimeModule(getRuntimeModule())
                .setDefaults();
    }
    
    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>, ?> getDefaultClientExecutor() {
        return LimitOutstandingClient.create(
                getRuntimeModule().getConfiguration(), 
                getClientBuilder().getConnectionClientExecutor());
    }
    
    protected abstract Runnable getDefaultRunnable();
}
