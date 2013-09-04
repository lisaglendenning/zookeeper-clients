package edu.uw.zookeeper.clients.trace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.clients.common.CallUntilPresent;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class TraceGeneratingClientBuilder<C extends TraceGeneratingClientBuilder<C>> extends TraceWritingClientBuilder<C> {


    public TraceGeneratingClientBuilder(ObjectMapper mapper,
            TraceWriterBuilder traceBuilder,
            TraceEventPublisherService tracePublisher,
            ClientBuilder clientBuilder, RuntimeModule runtime) {
        super(mapper, traceBuilder, tracePublisher, clientBuilder, runtime);
    }

    @Override
    protected Runnable getDefaultRunnable() {
        final CallUntilPresent<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                    CallUntilPresent.create(
                        IterationCallable.create(getRuntimeModule().getConfiguration(), 
                                SubmitCallable.create(getDefaultRequestGenerator(), getDefaultClientExecutor())));
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

    protected abstract Generator<Records.Request> getDefaultRequestGenerator();
}