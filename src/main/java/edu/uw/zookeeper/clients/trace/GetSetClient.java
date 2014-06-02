package edu.uw.zookeeper.clients.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class GetSetClient extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected GetSetClient(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<GetSetClient, GetSetClientBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(GetSetClientBuilder.defaults());
        }

        public MainBuilder(
                GetSetClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(GetSetClientBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected GetSetClient doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new GetSetClient(ServiceApplication.forService(monitor));
        }
    }
}
