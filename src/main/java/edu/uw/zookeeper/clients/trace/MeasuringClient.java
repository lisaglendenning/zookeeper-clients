package edu.uw.zookeeper.clients.trace;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class MeasuringClient extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected MeasuringClient(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<MeasuringClient, MeasuringClientBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(MeasuringClientBuilder.defaults());
        }

        public MainBuilder(
                MeasuringClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(MeasuringClientBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected MeasuringClient doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new MeasuringClient(ServiceApplication.newInstance(monitor));
        }
    }
}
