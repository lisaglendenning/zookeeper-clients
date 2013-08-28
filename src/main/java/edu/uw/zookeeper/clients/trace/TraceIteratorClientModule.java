package edu.uw.zookeeper.clients.trace;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.clients.common.SubmitIterator;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceIteratorClientModule extends TraceClientModule {

    public static ParameterizedFactory<RuntimeModule, Application> factory() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                try {
                    return new TraceIteratorClientModule(runtime).call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }  
        };
    }

    public class Module extends TraceClientModule.Module {

        @Provides @Singleton
        public Iterator<Records.Request> getRequests(
                Configuration configuration,
                ObjectMapper mapper) throws IOException {
            File file = new File(Trace.getTraceInputFileConfiguration(configuration));
            logger.info("Trace input: {}", file);
            Iterator<TraceEvent> events = TraceEventIterator.forFile(file, mapper.reader());
            return TraceRequestIterator.requestsOf(TraceRequestIterator.from(events));
        }
    }
    
    protected TraceIteratorClientModule(RuntimeModule runtime) {
        super(runtime);
    }

    @Override
    protected com.google.inject.Module module() {
        return new Module();
    }

    @Override
    protected Runnable getRunnable() {
        ClientExecutor<? super Operation.Request, Message.ServerResponse<?>> client = getClient();
        Iterator<Records.Request> requests = injector.getInstance(Key.get(new TypeLiteral<Iterator<Records.Request>>(){}));
        injector.getInstance(ServiceMonitor.class).add(injector.getInstance(TraceEventPublisherService.class));
        final Iterator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = SubmitIterator.create(requests, client);
        return new Runnable() {
            @Override
            public void run() {
                try {
                    Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> next = null;
                    while (operations.hasNext()) {
                        next = operations.next();
                    }
                    if (next != null) {
                        next.second().get();
                    }
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
