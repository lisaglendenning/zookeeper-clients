package edu.uw.zookeeper.clients.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class ThroughputClients extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected ThroughputClients(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<ThroughputClients, ThroughputClientsBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(ThroughputClientsBuilder.defaults());
        }

        public MainBuilder(
                ThroughputClientsBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(ThroughputClientsBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected ThroughputClients doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new ThroughputClients(ServiceApplication.forService(monitor));
        }
    }
}
