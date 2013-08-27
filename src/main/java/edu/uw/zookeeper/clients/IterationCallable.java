package edu.uw.zookeeper.clients;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

public class IterationCallable<V> implements Callable<Optional<V>> {

    public static <V> IterationCallable<V> create(
            Configuration configuration,
            Callable<V> callable) {
        return create(ConfigurableIterations.get(configuration), callable);
    }
    
    public static <V> IterationCallable<V> create(
            int iterations,
            Callable<V> callable) {
        return new IterationCallable<V>(iterations, callable);
    }

    @Configurable(arg="iterations", key="Iterations", value="5", type=ConfigValueType.NUMBER)
    public static class ConfigurableIterations implements Function<Configuration, Integer> {

        public static Integer get(Configuration configuration) {
            return new ConfigurableIterations().apply(configuration);
        }

        @Override
        public Integer apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getInt(configurable.key());
        }
    }

    protected final int iterations;
    protected final AtomicInteger count;
    protected final Callable<V> callable;
    
    public IterationCallable(
            int iterations,
            Callable<V> callable) {
        checkArgument(iterations >= 0);
        this.iterations = iterations;
        this.count = new AtomicInteger(0);
        this.callable = callable;
    }
    
    public int getIterations() {
        return iterations;
    }
    
    public int getCount() {
        return count.get();
    }
    
    @Override
    public Optional<V> call() throws Exception {
        final int count = this.count.incrementAndGet();
        checkState(count <= iterations);
        V result = callable.call();
        if (count < iterations) {
            return Optional.absent();
        } else {
            return Optional.of(result);
        }
    }
}