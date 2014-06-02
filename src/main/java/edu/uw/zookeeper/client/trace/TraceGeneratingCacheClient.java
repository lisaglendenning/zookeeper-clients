package edu.uw.zookeeper.client.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class TraceGeneratingCacheClient extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected TraceGeneratingCacheClient(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<TraceGeneratingCacheClient, TraceGeneratingCacheClientBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(TraceGeneratingCacheClientBuilder.defaults());
        }

        public MainBuilder(
                TraceGeneratingCacheClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(TraceGeneratingCacheClientBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected TraceGeneratingCacheClient doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new TraceGeneratingCacheClient(ServiceApplication.forService(monitor));
        }
    }
}
