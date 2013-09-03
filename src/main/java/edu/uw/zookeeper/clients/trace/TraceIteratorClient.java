package edu.uw.zookeeper.clients.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class TraceIteratorClient extends ZooKeeperApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected final Application application;
    
    protected TraceIteratorClient(Application application) {
        super();
        this.application = application;
    }

    @Override
    public void run() {
        application.run();
    }

    protected static class MainBuilder implements ZooKeeperApplication.RuntimeBuilder<TraceIteratorClient> {
        
        protected final TraceIteratorClientBuilder delegate;
        
        public MainBuilder() {
            this(TraceIteratorClientBuilder.defaults());
        }

        public MainBuilder(
                TraceIteratorClientBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return delegate.getRuntimeModule();
        }

        @Override
        public MainBuilder setRuntimeModule(
                RuntimeModule runtime) {
            return new MainBuilder(delegate.setRuntimeModule(runtime));
        }

        @Override
        public TraceIteratorClient build() {
            ServiceMonitor monitor = getRuntimeModule().serviceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new TraceIteratorClient(ServiceApplication.newInstance(monitor));
        }
    }
}
