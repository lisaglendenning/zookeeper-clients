package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.random.BasicRequestGenerator;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ParameterizedFactory;
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
    protected Generator<Records.Request> getRequestGenerator() {
        return BasicRequestGenerator.create(getCache());
    }
}
