package edu.uw.zookeeper.clients;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.SimpleClientBuilder;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;

public class SimpleClientsBuilder extends ClientConnectionExecutorsService.Builder {

    public static SimpleClientsBuilder defaults(
            ServerInetAddressView serverAddress,
            NetClientModule clientModule) {
        return new SimpleClientsBuilder(
                serverAddress, 
                connectionBuilder(clientModule), 
                null, 
                null, 
                null);
    }
    
    public static ClientConnectionFactoryBuilder connectionBuilder(
            NetClientModule clientModule) {
        return SimpleClientBuilder.connectionBuilder(clientModule);
    }

    protected final ServerInetAddressView serverAddress;

    protected SimpleClientsBuilder(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
            ClientConnectionExecutorsService<?> clientExecutors,
            RuntimeModule runtime) {
        super(connectionBuilder,clientConnectionFactory, clientExecutors, runtime);
        this.serverAddress = serverAddress;
    }

    public ServerInetAddressView getServerAddress() {
        return serverAddress;
    }
    
    public SimpleClientsBuilder setServerAddress(ServerInetAddressView serverAddress) {
        if (this.serverAddress == serverAddress) {
            return this;
        } else {
            return newInstance(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }
    }

    @Override
    public SimpleClientsBuilder setRuntimeModule(RuntimeModule runtime) {
        return (SimpleClientsBuilder) super.setRuntimeModule(runtime);
    }

    @Override
    public SimpleClientsBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return (SimpleClientsBuilder) super.setConnectionBuilder(connectionBuilder);
    }

    @Override
    public SimpleClientsBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory) {
        return (SimpleClientsBuilder) super.setClientConnectionFactory(clientConnectionFactory);
    }

    @Override
    public SimpleClientsBuilder setClientConnectionExecutors(
            ClientConnectionExecutorsService<?> clientExecutors) {
        return (SimpleClientsBuilder) super.setClientConnectionExecutors(clientExecutors);
    }

    @Override
    public SimpleClientsBuilder setDefaults() {
        return (SimpleClientsBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleClientsBuilder newInstance(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
            ClientConnectionExecutorsService<?> clientExecutors,
            RuntimeModule runtime) {
        return newInstance(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
    }
    
    protected SimpleClientsBuilder newInstance(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
            ClientConnectionExecutorsService<?> clientExecutors,
            RuntimeModule runtime) {
        return new SimpleClientsBuilder(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
    }

    @Override    
    protected ClientConnectionExecutorsService<?> getDefaultClientConnectionExecutorsService() {
        Factory<? extends ListenableFuture<? extends ClientConnectionExecutor<?>>> factory = 
                ServerViewFactory.newInstance(
                        clientConnectionFactory, 
                        serverAddress, 
                        getConnectionBuilder().getTimeOut(), 
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
        ClientConnectionExecutorsService<?> service =
                ClientConnectionExecutorsService.newInstance(
                        factory);
        return service;
    }
}
