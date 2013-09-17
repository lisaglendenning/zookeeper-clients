package edu.uw.zookeeper.clients;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
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
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.ClientBuilder.ConfigurableEnsembleView;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;

public class ClientConnectionExecutorsService<C extends ClientConnectionExecutor<?>> extends AbstractIdleService implements Factory<ListenableFuture<C>>, Function<C, C> {

    public static <C extends ClientConnectionExecutor<?>> ClientConnectionExecutorsService<C> newInstance(
            Factory<? extends ListenableFuture<? extends C>> factory) {
        return new ClientConnectionExecutorsService<C>(factory);
    }
    
    public static Builder builder() {
        return new Builder(null, null, null, null);
    }
    
    public static class Builder implements ZooKeeperApplication.RuntimeBuilder<List<Service>, Builder> {

        protected final RuntimeModule runtime;
        protected final ClientConnectionFactoryBuilder connectionBuilder;
        protected final ClientConnectionFactory<? extends ProtocolCodecConnection<Request, AssignXidCodec, Connection<Request>>> clientConnectionFactory;
        protected final ClientConnectionExecutorsService<?> clientExecutors;

        protected Builder(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
                ClientConnectionExecutorsService<?> clientExecutors,
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

        @Override
        public Builder setRuntimeModule(RuntimeModule runtime) {
            if (this.runtime == runtime) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
            }
        }

        public ClientConnectionFactoryBuilder getConnectionBuilder() {
            return connectionBuilder;
        }

        public Builder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
            if (this.connectionBuilder == connectionBuilder) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
            }
        }

        public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getClientConnectionFactory() {
            return clientConnectionFactory;
        }

        public Builder setClientConnectionFactory(
                ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
            if (this.clientConnectionFactory == clientConnectionFactory) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
            }
        }
        
        public ClientConnectionExecutorsService<?> getClientConnectionExecutors() {
            return clientExecutors;
        }

        public Builder setClientConnectionExecutors(
                ClientConnectionExecutorsService<?> clientExecutors) {
            if (this.clientExecutors == clientExecutors) {
                return this;
            } else {
                return newInstance(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
            }
        }

        @Override
        public Builder setDefaults() {
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
                return setClientConnectionExecutors(getDefaultClientConnectionExecutorsService()).setDefaults();
            }
            return this;
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
        
        protected Builder newInstance(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory,
                ClientConnectionExecutorsService<?> clientExecutors,
                RuntimeModule runtime) {
            return new Builder(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
            return ClientConnectionFactoryBuilder.defaults().setRuntimeModule(runtime).setDefaults();
        }

        protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
            return connectionBuilder.build();
        }

        protected ClientConnectionExecutorsService<?> getDefaultClientConnectionExecutorsService() {
            EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
            final EnsembleViewFactory<? extends ServerViewFactory<Session, ?>> ensembleFactory = 
                    EnsembleViewFactory.fromSession(
                        clientConnectionFactory,
                        ensemble, 
                        connectionBuilder.getTimeOut(),
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
            ClientConnectionExecutorsService<?> service =
                    ClientConnectionExecutorsService.newInstance(
                            new Factory<ListenableFuture<? extends ClientConnectionExecutor<?>>>() {
                                @Override
                                public ListenableFuture<? extends ClientConnectionExecutor<?>> get() {
                                    return ensembleFactory.get().get();
                                }
                            });
            return service;
        }
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
