package edu.uw.zookeeper.clients.trace;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.common.Configuration;

public class TraceGeneratorClient extends DefaultMain {

    public static void main(String[] args) {
        main(args, ConfigurableApplicationFactory.newInstance(TraceGeneratorClient.class));
    }

    public TraceGeneratorClient(Configuration configuration) {
        super(TraceGeneratorClientModule.factory(), configuration);
    }
}
