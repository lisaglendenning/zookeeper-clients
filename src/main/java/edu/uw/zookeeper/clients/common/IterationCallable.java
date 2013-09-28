package edu.uw.zookeeper.clients.common;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

/**
 * Not thread-safe.
 */
public class IterationCallable<V> implements Callable<Optional<V>> {

    public static <V> IterationCallable<V> create(
            Configuration configuration,
            Callable<V> callable) {
        int limit = ConfigurableIterations.get(configuration);
        int logIterations = ConfigurableLogIterations.get(configuration);
        int logInterval;
        if (logIterations > 0) {
            logInterval = limit / logIterations;
        } else {
            logInterval = 0;
        }
        return create(limit, logInterval, callable);
    }
    
    public static <V> IterationCallable<V> create(
            int limit,
            int logInterval,
            Callable<V> callable) {
        return new IterationCallable<V>(
                limit, logInterval, callable, LogManager.getLogger(IterationCallable.class));
    }

    public static abstract class ConfigurableInt implements Function<Configuration, Integer> {

        @Override
        public Integer apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getInt(configurable.key());
        }
    }
    
    @Configurable(arg="iterations", key="iterations", value="10", type=ConfigValueType.NUMBER)
    public static class ConfigurableIterations extends ConfigurableInt {

        public static Integer get(Configuration configuration) {
            return new ConfigurableIterations().apply(configuration);
        }
    }
    
    @Configurable(key="logIterations", value="10", type=ConfigValueType.NUMBER)
    public static class ConfigurableLogIterations extends ConfigurableInt {

        public static Integer get(Configuration configuration) {
            return new ConfigurableLogIterations().apply(configuration);
        }
    }
    
    protected static final int INFINITE_ITERATIONS = -1;

    protected final Logger logger;
    protected final int logInterval;
    protected final int limit;
    protected final Callable<V> callable;
    protected int count;
    
    public IterationCallable(
            int limit,
            int logInterval,
            Callable<V> callable,
            Logger logger) {
        checkArgument((limit >= 0) || (limit == INFINITE_ITERATIONS), limit);
        this.logger = logger;
        this.logInterval = logInterval;
        this.limit = limit;
        this.callable = callable;
        this.count = 0;
    }
    
    public int getLimit() {
        return limit;
    }
    
    public int getCount() {
        return count;
    }
    
    @Override
    public Optional<V> call() throws Exception {
        if ((limit >=0) && (count >= limit)) {
            return Optional.absent();
        }
        this.count++;
        if ((logInterval != 0) && ((count == 1) || (count == limit) || (count % logInterval == 0))) {
            logger.info("Iteration {}", count);
        }
        V result = callable.call();
        if ((limit == INFINITE_ITERATIONS) || (count < limit)) {
            return Optional.absent();
        } else {
            return Optional.of(result);
        }
    }
}