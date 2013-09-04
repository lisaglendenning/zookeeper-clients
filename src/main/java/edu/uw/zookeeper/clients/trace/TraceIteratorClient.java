package edu.uw.zookeeper.clients.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class TraceIteratorClient extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected TraceIteratorClient(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<TraceIteratorClient, TraceIteratorClientBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(TraceIteratorClientBuilder.defaults());
        }

        public MainBuilder(
                TraceIteratorClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(TraceIteratorClientBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected TraceIteratorClient doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new TraceIteratorClient(ServiceApplication.newInstance(monitor));
        }
    }
}
