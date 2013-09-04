package edu.uw.zookeeper.clients.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class TraceIteratingClient extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected TraceIteratingClient(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<TraceIteratingClient, TraceIteratingClientBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(TraceIteratingClientBuilder.defaults());
        }

        public MainBuilder(
                TraceIteratingClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(TraceIteratingClientBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected TraceIteratingClient doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new TraceIteratingClient(ServiceApplication.newInstance(monitor));
        }
    }
}
