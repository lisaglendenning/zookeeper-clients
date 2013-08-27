package edu.uw.zookeeper.clients.trace;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.common.ConfigurableMain;
import edu.uw.zookeeper.common.Configuration;

public class MeasuringClient extends DefaultMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.ConfigurableApplicationFactory.newInstance(MeasuringClient.class));
    }

    public MeasuringClient(Configuration configuration) {
        super(MeasuringClientModule.factory(), configuration);
    }
}
