package edu.uw.zookeeper.clients;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.References;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ConfigurableEnsembleView;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;

public class ConnectionClientExecutorsService
        <I extends Operation.Request, T, C extends ConnectionClientExecutor<I,?,?>> 
        extends AbstractIdleService 
        implements DefaultsFactory<T, ListenableFuture<C>>, Function<C, C>, Iterable<C> {

    public static <I extends Operation.Request, T, C extends ConnectionClientExecutor<I,?,?>> ConnectionClientExecutorsService<I,T,C> newInstance(
            DefaultsFactory<T, ? extends ListenableFuture<? extends C>> factory) {
        return new ConnectionClientExecutorsService<I,T,C>(factory);
    }
    
    public static OperationBuilder builder() {
        return new OperationBuilder(null, null, null, null);
    }

    public static abstract class AbstractBuilder<T extends ConnectionClientExecutorsService<?,?,?>, C extends AbstractBuilder<T,C>> implements ZooKeeperApplication.RuntimeBuilder<List<Service>, C> {

        protected final RuntimeModule runtime;
        protected final ClientConnectionFactoryBuilder connectionBuilder;
        protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory;
        protected final T clientExecutors;

        protected AbstractBuilder(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                T clientExecutors,
                RuntimeModule runtime) {
            this.connectionBuilder = connectionBuilder;
            this.clientConnectionFactory = clientConnectionFactory;
            this.clientExecutors = clientExecutors;
            this.runtime = runtime;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setRuntimeModule(RuntimeModule runtime) {
            if (this.runtime == runtime) {
                return (C) this;
            } else {
                return newInstance(
                        (connectionBuilder == null) ? connectionBuilder : connectionBuilder.setRuntimeModule(runtime), 
                        clientConnectionFactory, clientExecutors, runtime);
            }
        }

        public ClientConnectionFactoryBuilder getConnectionBuilder() {
            return connectionBuilder;
        }

        @SuppressWarnings("unchecked")
        public C setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
            if (this.connectionBuilder == connectionBuilder) {
                return (C) this;
            } else {
                return newInstance(connectionBuilder.setRuntimeModule(getRuntimeModule()), clientConnectionFactory, clientExecutors, runtime);
            }
        }

        public ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getClientConnectionFactory() {
            return clientConnectionFactory;
        }

        @SuppressWarnings("unchecked")
        public C setClientConnectionFactory(
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory) {
            if (this.clientConnectionFactory == clientConnectionFactory) {
                return (C) this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
            }
        }
        
        public T getConnectionClientExecutors() {
            return clientExecutors;
        }

        @SuppressWarnings("unchecked")
        public C setConnectionClientExecutors(
                T clientExecutors) {
            if (this.clientExecutors == clientExecutors) {
                return (C) this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setDefaults() {
            checkState(getRuntimeModule() != null);
        
            if (this.connectionBuilder == null) {
                return setConnectionBuilder(getDefaultClientConnectionFactoryBuilder()).setDefaults();
            }
            ClientConnectionFactoryBuilder connectionBuilder = this.connectionBuilder.setDefaults();
            if (this.connectionBuilder != connectionBuilder) {
                return setConnectionBuilder(connectionBuilder).setDefaults();
            }
            if (clientConnectionFactory == null) {
                return setClientConnectionFactory(getDefaultClientConnectionFactory()).setDefaults();
            }
            if (clientExecutors == null) {
                return setConnectionClientExecutors(getDefaultConnectionClientExecutorsService()).setDefaults();
            }
            return (C) this;
        }

        @Override
        public List<Service> build() {
            return setDefaults().doBuild();
        }

        protected List<Service> doBuild() {
            return Lists.<Service>newArrayList(
                    clientConnectionFactory, 
                    clientExecutors);
        }
        
        protected abstract C newInstance(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                T clientExecutors,
                RuntimeModule runtime);

        protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
            return ClientConnectionFactoryBuilder.defaults()
                    .setRuntimeModule(runtime).setDefaults();
        }

        protected ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> getDefaultClientConnectionFactory() {
            return connectionBuilder.build();
        }

        protected abstract T getDefaultConnectionClientExecutorsService();
    }
    
    public static class OperationBuilder extends AbstractBuilder<ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>>, OperationBuilder> {

        protected OperationBuilder(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> clientExecutors,
                RuntimeModule runtime) {
            super(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        @Override
        protected OperationBuilder newInstance(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> clientExecutors,
                RuntimeModule runtime) {
            return new OperationBuilder(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        @Override
        protected ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> getDefaultConnectionClientExecutorsService() {
            EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
            final EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<?>>> ensembleFactory = 
                    EnsembleViewFactory.fromSession(
                        clientConnectionFactory,
                        ensemble, 
                        connectionBuilder.getTimeOut(),
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
            ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> service =
                    ConnectionClientExecutorsService.newInstance(
                            new DefaultsFactory<Session, ListenableFuture<? extends OperationClientExecutor<?>>>() {
                                @Override
                                public ListenableFuture<? extends OperationClientExecutor<?>> get(Session value) {
                                    return ensembleFactory.get().get(value);
                                }
                                @Override
                                public ListenableFuture<? extends OperationClientExecutor<?>> get() {
                                    return ensembleFactory.get().get();
                                }
                            });
            return service;
        }
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Executor executor;
    protected final DefaultsFactory<T, ? extends ListenableFuture<? extends C>> factory;
    protected final Set<C> executors;
    
    protected ConnectionClientExecutorsService(
            DefaultsFactory<T, ? extends ListenableFuture<? extends C>> factory) {
        this.executor = MoreExecutors.sameThreadExecutor();
        this.factory = factory;
        this.executors = Collections.synchronizedSet(Sets.<C>newHashSet());
    }

    @Override
    public ListenableFuture<C> get(T value) {
        checkState(isRunning());
        return Futures.transform(factory.get(value), this, executor);
    }

    @Override
    public ListenableFuture<C> get() {
        checkState(isRunning());
        return Futures.transform(factory.get(), this, executor);
    }

    @Override
    public C apply(C input) {
        new ClientHandler(input);
        return input;
    }

    @Override
    public Iterator<C> iterator() {
        ImmutableSet.Builder<C> copy = ImmutableSet.builder();
        synchronized (executors) {
            copy.addAll(executors);
        }
        return copy.build().iterator();
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        @SuppressWarnings("unchecked")
        I disconnect = (I) ProtocolRequestMessage.of(0, 
                Operations.Requests.disconnect().build());
        List<Pair<C, ListenableFuture<? extends Operation.ProtocolResponse<?>>>> futures = 
                Lists.newArrayListWithExpectedSize(executors.size());
        for (C c: this) {
            ListenableFuture<? extends Operation.ProtocolResponse<?>> future = null;
            try {
                if ((c.connection().codec().state() == ProtocolState.CONNECTED) && 
                        (c.connection().state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
                    future = c.submit(disconnect);
                } else {
                    future = null;
                }
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            futures.add(Pair.<C, ListenableFuture<? extends Operation.ProtocolResponse<?>>>create(c, future));
        }
        
        for (Pair<C, ListenableFuture<? extends Operation.ProtocolResponse<?>>> future: futures) {
            try {
                if (future.second() != null) {
                    int timeOut = future.first().session().get().getTimeOut();
                    if (timeOut > 0) {
                        future.second().get(timeOut, TimeUnit.MILLISECONDS);
                    } else {
                        future.second().get();
                    }
                }
            } catch (Exception e) {
                logger.debug("", e);
            } finally {
                future.first().connection().close();
            }
        }
    }
    
    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected class ClientHandler implements Reference<C> {
        
        protected final C instance;
        
        public ClientHandler(C instance) {
            this.instance = instance;
            executors.add(instance);
            instance.subscribe(this);
            if (! isRunning()) {
                instance.connection().close();
                throw new IllegalStateException(String.valueOf(state()));
            }
        }
        
        @Override
        public C get() {
            return instance;
        }

        @Handler
        public void handleTransition(Automaton.Transition<?> event) {
            if (event.to() == Connection.State.CONNECTION_CLOSED) {
                try {
                    instance.unsubscribe(this);
                } catch (IllegalArgumentException e) {}
                executors.remove(instance);
            }
        }
    }
}
