package edu.uw.zookeeper.clients.trace;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.common.Configuration;

public class TraceIteratorClient extends DefaultMain {

    public static void main(String[] args) {
        main(args, ConfigurableApplicationFactory.newInstance(TraceIteratorClient.class));
    }

    public TraceIteratorClient(Configuration configuration) {
        super(TraceIteratorClientModule.factory(), configuration);
    }
}
