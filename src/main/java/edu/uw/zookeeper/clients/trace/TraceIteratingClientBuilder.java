package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.clients.common.SubmitIterator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceIteratingClientBuilder extends TraceWritingClientBuilder<TraceIteratingClientBuilder> {

    public static TraceIteratingClientBuilder defaults() {
        return new TraceIteratingClientBuilder();
    }

    public TraceIteratingClientBuilder() {
        this(null, null, null, null, null);
    }

    public TraceIteratingClientBuilder(
            ClientConnectionExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    @Override
    protected TraceIteratingClientBuilder newInstance(
            ClientConnectionExecutorService.Builder clientBuilder,
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return new TraceIteratingClientBuilder(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    @Override
    protected Runnable getDefaultRunnable() {
        ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> client = getDefaultClientExecutor();
        Iterator<Records.Request> requests = getDefaultRequests();
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

    protected Iterator<Records.Request> getDefaultRequests() {
        ObjectReader reader = mapper.reader();
        File file = Tracing.getTraceInputFileConfiguration(getRuntimeModule().getConfiguration());
        logger.info("Trace input: {}", file);
        Iterator<TraceEvent> events;
        try {
            events = TraceEventIterator.forFile(file, reader);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return TraceRequestIterator.requestsOf(TraceRequestIterator.from(events));
    }
}
