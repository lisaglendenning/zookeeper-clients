package edu.uw.zookeeper.clients;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class ClientConnectionExecutorsService<C extends ClientConnectionExecutor<?>> extends AbstractIdleService implements Factory<ListenableFuture<C>>, Function<C, C> {

    public static <C extends ClientConnectionExecutor<?>> ClientConnectionExecutorsService<C> newInstance(
            Factory<? extends ListenableFuture<? extends C>> factory) {
        return new ClientConnectionExecutorsService<C>(factory);
    }
    
    protected final Executor executor;
    protected final Factory<? extends ListenableFuture<? extends C>> factory;
    protected final Set<C> executors;
    
    protected ClientConnectionExecutorsService(
            Factory<? extends ListenableFuture<? extends C>> factory) {
        this.executor = MoreExecutors.sameThreadExecutor();
        this.factory = factory;
        this.executors = Collections.synchronizedSet(Sets.<C>newHashSet());
    }
    
    @Override
    public ListenableFuture<C> get() {
        return Futures.transform(factory.get(), this, executor);
    }

    @Override
    public C apply(C input) {
        executors.add(input);
        return input;
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        synchronized (executors) {
            for (C e: executors) {
                try {
                    if ((e.get().codec().state() == ProtocolState.CONNECTED) && 
                            (e.get().state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
                        e.submit(Records.Requests.getInstance().get(OpCode.CLOSE_SESSION)).get(e.session().get().getTimeOut(), TimeUnit.MILLISECONDS);
                    }
                } finally {
                    e.stop();
                }
            }
        }
    }
}
