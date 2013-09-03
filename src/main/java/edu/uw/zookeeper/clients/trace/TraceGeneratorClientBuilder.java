package edu.uw.zookeeper.clients.trace;

import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.random.BasicRequestGenerator;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceGeneratorClientBuilder extends TraceWriterClientBuilder<TraceGeneratorClientBuilder> {

    public static TraceGeneratorClientBuilder defaults() {
        return new TraceGeneratorClientBuilder();
    }
    
    protected TraceGeneratorClientBuilder() {
        this(null, null, null);
    }

    protected TraceGeneratorClientBuilder(
            Injector injector,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        super(injector, connectionBuilder, clientConnectionFactory);
    }

    @Override
    public TraceGeneratorClientBuilder setInjector(Injector injector) {
        return new TraceGeneratorClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }

    @Override
    public TraceGeneratorClientBuilder setRuntimeModule(RuntimeModule runtime) {
        return new TraceGeneratorClientBuilder(injector, connectionBuilder.setRuntimeModule(runtime), clientConnectionFactory);
    }
    
    @Override
    public TraceGeneratorClientBuilder setConnectionBuilder(ClientConnectionFactoryBuilder connectionBuilder) {
        return new TraceGeneratorClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    public TraceGeneratorClientBuilder setClientConnectionFactory(
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        return new TraceGeneratorClientBuilder(injector, connectionBuilder, clientConnectionFactory);
    }
    
    @Override
    protected Generator<Records.Request> getDefaultRequestGenerator() {
        return BasicRequestGenerator.create(getCache());
    }

    @Override
    protected Injector getDefaultInjector() {
        return Guice.createInjector(new Module());
    }
}
