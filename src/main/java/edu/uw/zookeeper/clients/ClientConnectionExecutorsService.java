package edu.uw.zookeeper.clients;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;

public class ClientConnectionExecutorsService<C extends ClientConnectionExecutor<?>> extends AbstractIdleService implements Factory<ListenableFuture<C>>, Function<C, C> {

    public static <C extends ClientConnectionExecutor<?>> ClientConnectionExecutorsService<C> newInstance(
            Factory<? extends ListenableFuture<? extends C>> factory) {
        return new ClientConnectionExecutorsService<C>(factory);
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
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
        Operations.Requests.Disconnect disconnect = Operations.Requests.disconnect();
        synchronized (executors) {
            for (C c: Iterables.consumingIterable(executors)) {
                try {
                    if ((c.get().codec().state() == ProtocolState.CONNECTED) && 
                            (c.get().state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
                        ListenableFuture<Message.ServerResponse<?>> future = c.submit(disconnect.build());
                        int timeOut = c.session().get().getTimeOut();
                        if (timeOut > 0) {
                            future.get(timeOut, TimeUnit.MILLISECONDS);
                        } else {
                            future.get();
                        }
                    }
                } catch (Exception e) {
                    logger.debug("", e);
                    throw e;
                } finally {
                    c.stop();
                }
            }
        }
    }
}
