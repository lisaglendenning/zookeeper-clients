package edu.uw.zookeeper.clients;

import java.util.concurrent.ScheduledExecutorService;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.SimpleClientBuilder;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;

public class SimpleClientsBuilder extends ConnectionClientExecutorsService.OperationBuilder {

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
            ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> clientExecutors,
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
    public SimpleClientsBuilder setConnectionClientExecutors(
            ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> clientExecutors) {
        return (SimpleClientsBuilder) super.setConnectionClientExecutors(clientExecutors);
    }

    @Override
    public SimpleClientsBuilder setDefaults() {
        return (SimpleClientsBuilder) super.setDefaults();
    }
    
    @Override
    protected SimpleClientsBuilder newInstance(
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
            ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> clientExecutors,
            RuntimeModule runtime) {
        return newInstance(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
    }
    
    protected SimpleClientsBuilder newInstance(
            ServerInetAddressView serverAddress,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
            ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> clientExecutors,
            RuntimeModule runtime) {
        return new SimpleClientsBuilder(serverAddress, connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
    }

    @Override    
    protected ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> getDefaultConnectionClientExecutorsService() {
        ServerViewFactory<Session, ? extends OperationClientExecutor<?>> factory = 
                ServerViewFactory.newInstance(
                        clientConnectionFactory, 
                        serverAddress, 
                        getConnectionBuilder().getTimeOut(), 
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
        ConnectionClientExecutorsService<Operation.Request, Session, OperationClientExecutor<?>> service =
                ConnectionClientExecutorsService.newInstance(
                        factory);
        return service;
    }
}
