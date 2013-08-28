package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Throwables;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.clients.Generator;
import edu.uw.zookeeper.clients.random.BasicRequestGenerator;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.proto.Records;

public class TraceGeneratorClientModule extends TraceWriterClientModule {

    public static ParameterizedFactory<RuntimeModule, Application> factory() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                try {
                    return new TraceGeneratorClientModule(runtime).call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }  
        };
    }

    protected TraceGeneratorClientModule(RuntimeModule runtime) {
        super(runtime);
    }

    @Override
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return ProtocolTracingCodec.factory(injector.getInstance(Publisher.class));
    }

    @Override
    protected Generator<Records.Request> getRequestGenerator() {
        return BasicRequestGenerator.create(getCache());
    }
}
