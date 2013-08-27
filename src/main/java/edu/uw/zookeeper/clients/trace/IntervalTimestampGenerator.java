package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AbstractScheduledService;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TimeValue;

public class IntervalTimestampGenerator extends AbstractScheduledService {

    public static IntervalTimestampGenerator create(Publisher publisher, Configuration configuration) {
        return new IntervalTimestampGenerator(publisher, ConfigurableTimestampInterval.get(configuration));
    }
    
    @Configurable(arg="timestamp", key="Timestamp", value="500 ms", help="Time")
    public static class ConfigurableTimestampInterval implements Function<Configuration, TimeValue> {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimestampInterval().apply(configuration);
        }

        @Override
        public TimeValue apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }
    }

    protected final Scheduler scheduler;
    protected final Publisher publisher;
    
    protected IntervalTimestampGenerator(
            Publisher publisher,
            TimeValue interval) {
        this.publisher = publisher;
        this.scheduler = Scheduler.newFixedRateSchedule(0, interval.value(), interval.unit());
    }
    
    @Override
    protected void runOneIteration() throws Exception {
        publisher.post(TimestampEvent.currentTimeMillis());
    }

    @Override
    protected Scheduler scheduler() {
        return scheduler;
    }
    
    @Override
    protected void shutDown() throws Exception {
        runOneIteration();
        super.shutDown();
    }
}
