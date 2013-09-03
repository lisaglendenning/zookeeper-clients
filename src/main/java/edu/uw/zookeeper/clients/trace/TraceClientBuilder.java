package edu.uw.zookeeper.clients.trace;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.internal.Lists;

import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LimitOutstandingClient;
import edu.uw.zookeeper.clients.ApplicationModule;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;

public abstract class TraceClientBuilder<C extends TraceClientBuilder<C>> extends ClientBuilder {

    public class Module extends ApplicationModule<C> {

        @SuppressWarnings("unchecked")
        protected Module() {
            super((C) TraceClientBuilder.this);
        }
        
        @Override
        protected void configure() {
            super.configure();
            bind(new TypeLiteral<Actor<TraceEvent>>() {}).to(TraceWriter.class);
        }

        @Provides @Singleton
        public TraceHeader getTraceHeader() {
            return builder.getDefaultTraceHeader();
        }
        
        @Provides @Singleton
        public ClientConnectionExecutorService getClientConnectionExecutorService() {
            return builder.getDefaultClientConnectionExecutorService();
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            List<com.google.inject.Module> modules = super.getDependentModules();
            modules.add(TraceModule.create());
            return modules;
        }
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Injector injector;

    protected TraceClientBuilder(
            Injector injector,
            ClientConnectionFactoryBuilder connectionBuilder,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clientConnectionFactory) {
        super(connectionBuilder, clientConnectionFactory);
        this.injector = injector;
    }

    public Injector getInjector() {
        return injector;
    }

    public abstract C setInjector(Injector injector);

    @SuppressWarnings("unchecked")
    @Override
    public C setDefaults() {
        if (injector == null) {
            return setInjector(getDefaultInjector());
        } else {
            return (C) super.setDefaults();
        }
    }

    @Override
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> getDefaultClientConnectionFactory() {
        return connectionBuilder.setCodecFactory(
                ProtocolTracingCodec.factory(injector.getInstance(Publisher.class))).build();
    }
    
    protected abstract Injector getDefaultInjector();

    protected ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> getDefaultClientExecutor() {
        return LimitOutstandingClient.create(
                getRuntimeModule().configuration(), 
                injector.getInstance(ClientConnectionExecutorService.class));
    }
    
    protected TraceHeader getDefaultTraceHeader() {
        return TraceHeader.create(
                TraceModule.TraceDescriptionConfiguration.get(getRuntimeModule().configuration()), 
                TraceEventTag.TIMESTAMP_EVENT, 
                TraceEventTag.PROTOCOL_REQUEST_EVENT, 
                TraceEventTag.PROTOCOL_RESPONSE_EVENT);
    }

    @Override
    protected List<Service> getServices() {
        return Lists.<Service>newArrayList(
                clientConnectionFactory, 
                injector.getInstance(ClientConnectionExecutorService.class));
    }
}
