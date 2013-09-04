package edu.uw.zookeeper.clients.trace;


import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.clients.common.RunnableService;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;

public abstract class TraceWritingClientBuilder<C extends TraceWritingClientBuilder<C>> implements ZooKeeperApplication.RuntimeBuilder<List<Service>, C> {

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final RuntimeModule runtime;
    protected final ObjectMapper mapper;
    protected final TraceWriterBuilder traceBuilder;
    protected final TraceEventPublisherService tracePublisher;
    protected final ClientBuilder clientBuilder;

    protected TraceWritingClientBuilder() {
        this(null, null, null, null, null);
    }

    protected TraceWritingClientBuilder(
            ObjectMapper mapper,
            TraceWriterBuilder traceBuilder,
            TraceEventPublisherService tracePublisher,
            ClientBuilder clientBuilder,
            RuntimeModule runtime) {
        this.mapper = mapper;
        this.runtime = runtime;
        this.traceBuilder = traceBuilder;
        this.tracePublisher = tracePublisher;
        this.clientBuilder = clientBuilder;
    }
    
    @Override
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Override
    public C setRuntimeModule(RuntimeModule runtime) {
        return newInstance(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    public C setObjectMapper(ObjectMapper mapper) {
        return newInstance(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    public C setTracePublisher(
            TraceEventPublisherService tracePublisher) {
        return newInstance(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    public C setTraceBuilder(TraceWriterBuilder traceBuilder) {
        return newInstance(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    public C setClientBuilder(ClientBuilder clientBuilder) {
        return newInstance(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    @SuppressWarnings("unchecked")
    public C setDefaults() {
        checkState(getRuntimeModule() != null);
        
        if (mapper == null) {
            return setObjectMapper(getDefaultObjectMapper()).setDefaults();
        }
        if (traceBuilder == null) {
            return setTraceBuilder(getDefaultTraceBuilder()).setDefaults();
        }
        if (tracePublisher == null) {
            return setTracePublisher(getDefaultTraceEventPublisherService()).setDefaults();
        }
        if (clientBuilder == null) {
            return setClientBuilder(getDefaultClientBuilder()).setDefaults();
        }
        return (C) this;
    }

    @Override
    public List<Service> build() {
        return setDefaults().getServices();
    }
    
    protected abstract C newInstance(
    ObjectMapper mapper,
    TraceWriterBuilder traceBuilder,
    TraceEventPublisherService tracePublisher,
    ClientBuilder clientBuilder,
    RuntimeModule runtime);

    protected List<Service> getServices() {
        List<Service> services = Lists.newLinkedList();
        services.add(tracePublisher);
        services.addAll(clientBuilder.build());
        services.add(RunnableService.create(getDefaultRunnable()));
        return services;
    }
    
    protected ObjectMapper getDefaultObjectMapper() {
        return ObjectMapperBuilder.defaults().build();
    }
    
    protected TraceWriterBuilder getDefaultTraceBuilder() {
        ObjectWriter writer = mapper.writer();
        File file = Trace.getTraceOutputFileConfiguration(getRuntimeModule().getConfiguration());
        logger.info("Trace output: {}", file);
        Executor executor = getRuntimeModule().getExecutors().get(ExecutorService.class);
        TraceHeader header = getDefaultTraceHeader();
        return TraceWriterBuilder.defaults()
                .setHeader(header)
                .setWriter(writer)
                .setExecutor(executor)
                .setFile(file);
    }

    protected TraceEventPublisherService getDefaultTraceEventPublisherService() {
        return TraceEventPublisherService.newInstance(
                EventBusPublisher.newInstance(), traceBuilder.build());
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
    
    protected TraceHeader getDefaultTraceHeader() {
        return TraceHeader.create(
                Trace.getTraceDescription(getRuntimeModule().getConfiguration()), 
                TraceEventTag.TIMESTAMP_EVENT, 
                TraceEventTag.PROTOCOL_REQUEST_EVENT, 
                TraceEventTag.PROTOCOL_RESPONSE_EVENT);
    }
    
    protected abstract Runnable getDefaultRunnable();
}
