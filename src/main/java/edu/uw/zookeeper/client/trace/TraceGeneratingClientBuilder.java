package edu.uw.zookeeper.client.trace;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.IteratingClient;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class TraceGeneratingClientBuilder<C extends TraceGeneratingClientBuilder<C>> extends TraceWritingClientBuilder<C> {

    protected TraceGeneratingClientBuilder(
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    @Override
    protected Runnable getDefaultRunnable() {
        final IteratingClient callable = IteratingClient.create(
                getRuntimeModule().getExecutors().get(ExecutorService.class), 
                CountingGenerator.fromConfiguration(
                        getRuntimeModule().getConfiguration(), 
                        SubmitGenerator.create(
                                getDefaultRequestGenerator(), 
                                getDefaultClientExecutor())),
                SettableFuturePromise.<Void>create());
        LoggingFutureListener.listen(logger, callable);
        return new Runnable() {
            @Override
            public void run() {
                getRuntimeModule().getExecutors().get(ExecutorService.class).execute(callable);
                try {
                    callable.get();
                } catch (InterruptedException e) {
                    return;
                } catch (ExecutionException e) {
                    logger.error("", e.getCause());
                }
            }
        };
    }

    protected abstract Generator<? extends Records.Request> getDefaultRequestGenerator();
}
