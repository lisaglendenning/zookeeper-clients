package edu.uw.zookeeper.clients;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.internal.Sets;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class ClientConnectionExecutorsService<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends AbstractIdleService implements Factory<ClientConnectionExecutor<C>> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ClientConnectionExecutorsService<C> create(
            Factory<ClientConnectionExecutor<C>> factory) {
        return new ClientConnectionExecutorsService<C>(factory);
    }
    
    protected final Factory<ClientConnectionExecutor<C>> factory;
    protected final Set<ClientConnectionExecutor<C>> executors;
    
    protected ClientConnectionExecutorsService(
            Factory<ClientConnectionExecutor<C>> factory) {
        this.factory = factory;
        this.executors = Collections.synchronizedSet(Sets.<ClientConnectionExecutor<C>>newHashSet());
    }
    
    @Override
    public ClientConnectionExecutor<C> get() {
        return factory.get();
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        for (ClientConnectionExecutor<C> e: executors) {
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
