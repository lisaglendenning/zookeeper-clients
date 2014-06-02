package edu.uw.zookeeper.client.trace;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.IteratingClient;
import edu.uw.zookeeper.client.SubmitIterator;
import edu.uw.zookeeper.client.random.BinGenerator;
import edu.uw.zookeeper.client.random.PathRequestGenerator;
import edu.uw.zookeeper.client.random.RandomData;
import edu.uw.zookeeper.client.random.RandomFromList;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Generators;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.Records;

public class GetSetClientBuilder extends MeasuringClientBuilder {

    public static GetSetClientBuilder defaults() {
        return new GetSetClientBuilder();
    }

    @Configurable(arg="gets", key="getPercentage", value="0.66", type=ConfigValueType.NUMBER, help="percentage get/put")
    public static class GetPercentageConfiguration implements Function<Configuration, Float> {

        public static Float get(Configuration configuration) {
            return new GetPercentageConfiguration().apply(configuration);
        }

        @Override
        public Float apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return Float.valueOf(
                    Double.valueOf(configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getDouble(configurable.key())).floatValue());
        }
    }

    @Configurable(arg="data", key="dataMaxMB", value="0.1", type=ConfigValueType.NUMBER, help="znode data max MB")
    public static class DataMaxMBConfiguration implements Function<Configuration, Float> {

        public static Float get(Configuration configuration) {
            return new DataMaxMBConfiguration().apply(configuration);
        }

        @Override
        public Float apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return Float.valueOf(
                    Double.valueOf(configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getDouble(configurable.key())).floatValue());
        }
    }

    public static final class PerfectTreeParameters {

        public static PerfectTreeParameters fromConfiguration(Configuration configuration) {
            Configurable configurable;
            try {
                configurable = PerfectTreeParameters.class.getDeclaredField("branching").getAnnotation(Configurable.class);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            int branching = configuration.withConfigurable(configurable)
                .getConfigOrEmpty(configurable.path())
                    .getInt(configurable.key());
            checkArgument(branching > 0);
            try {
                configurable = PerfectTreeParameters.class.getDeclaredField("depth").getAnnotation(Configurable.class);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            int depth = configuration.withConfigurable(configurable)
                .getConfigOrEmpty(configurable.path())
                    .getInt(configurable.key());
            checkArgument(depth > 0);
            return new PerfectTreeParameters(branching, depth);
        }

        @Configurable(arg="branching", path="data", key="branching", value="2", type=ConfigValueType.NUMBER)
        private final int branching;

        @Configurable(arg="depth", path="data", key="depth", value="3", type=ConfigValueType.NUMBER)
        private final int depth;
        
        protected PerfectTreeParameters(int branching, int depth) {
            this.branching = branching;
            this.depth = depth;
        }
        
        public int getBranching() {
            return branching;
        }
        
        public int getDepth() {
            return depth;
        }
    }
    
    /**
     * Depth-first.
     * 
     * Not thread-safe
     */
    public static final class PerfectTreePaths extends AbstractIterator<AbsoluteZNodePath> {

        public static PerfectTreePaths forParameters(PerfectTreeParameters parameters) {
            return new PerfectTreePaths(parameters);
        }

        private final PerfectTreeParameters parameters;
        private final List<Integer> finger;
        
        protected PerfectTreePaths(PerfectTreeParameters parameters) {
            this.parameters = parameters;
            this.finger = Lists.newArrayListWithCapacity(parameters.getDepth());
            this.finger.add(Integer.valueOf(0));
        }

        @Override
        protected AbsoluteZNodePath computeNext() {
            if (finger.isEmpty()) {
                return endOfData();
            }
            ZNodeName relative;
            if (finger.size() == 1) {
                relative = ZNodeLabel.fromString(String.valueOf(finger.get(0)));
            } else {
                relative = RelativeZNodePath.fromString(
                        ZNodeLabelVector.join(
                            Iterators.transform(
                                    finger.iterator(), 
                                    Functions.toStringFunction())));
            }
            int index = finger.size() - 1;
            if (index < parameters.getDepth() - 1) {
                // depth
                finger.add(Integer.valueOf(0));
            } else {
                // backtrack
                int child;
                do {
                    child = finger.get(index).intValue();
                    if (child == parameters.getBranching() - 1) {
                        finger.remove(index);
                        index -= 1;
                    } else {
                        break;
                    }
                } while (index >= 0);
                // branch
                if (index >= 0) {
                    finger.set(index, Integer.valueOf(child + 1));
                }
            }
            return (AbsoluteZNodePath) ZNodePath.root().join(relative);
        }
    }

    public static class SetDataGenerator implements Generator<Operations.Requests.SetData> {
        
        public static SetDataGenerator forData(Generator<byte[]> data) {
            return new SetDataGenerator(data);
        }
        
        protected final Generator<byte[]> data;
        
        protected SetDataGenerator(Generator<byte[]> data) {
            this.data = data;
        }
        
        @Override
        public Operations.Requests.SetData next() {
            return Operations.Requests.setData().setData(data.next());
        }
    }
    
    protected final Random random;
    protected final PerfectTreeParameters parameters;

    protected GetSetClientBuilder() {
        this(null, null, null, null, null, null, null);
    }

    protected GetSetClientBuilder(
            Random random,
            PerfectTreeParameters parameters,
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        super(clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
        this.random = random;
        this.parameters = parameters;
    }
    
    public Random getRandom() {
        return random;
    }
    
    public GetSetClientBuilder setRandom(
            Random random) {
        return newInstance(random, parameters, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    public PerfectTreeParameters getParameters() {
        return parameters;
    }

    public GetSetClientBuilder setParameters(
            PerfectTreeParameters parameters) {
        return newInstance(random, parameters, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }
    
    @Override
    public GetSetClientBuilder setDefaults() {
        GetSetClientBuilder builder = (GetSetClientBuilder) super.setDefaults();
        if (this == builder) {
            if (getRandom() == null) {
                return setRandom(getDefaultRandom());
            }
            if (getParameters() == null) {
                return setParameters(getDefaultParameters());
            }
        }
        return builder;
    }

    @Override
    protected GetSetClientBuilder newInstance(
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return newInstance(random, parameters, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    protected GetSetClientBuilder newInstance(
            Random random,
            PerfectTreeParameters parameters,
            ConnectionClientExecutorService.Builder clientBuilder, 
            TraceWriterBuilder writerBuilder,
            TraceEventPublisherService tracePublisher,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        return new GetSetClientBuilder(random, parameters, clientBuilder, writerBuilder, tracePublisher, mapper, runtime);
    }

    @Override
    protected Runnable getDefaultRunnable() {
        // this is hacky, but we're doing it this way because we
        // don't want the tree creation operations to go through
        // the tracing layer
        final Runnable delegate = super.getDefaultRunnable();
        final ConnectionClientExecutorService.Builder untraced = 
                ConnectionClientExecutorService.builder()
                .setConnectionBuilder(
                        getClientBuilder().getConnectionBuilder().setCodecFactory(null).setDefaults())
                .setRuntimeModule(getRuntimeModule())
                .setDefaults();
        return new Runnable() {
            @Override
            public void run() {
                for (Service service: untraced.build()) {
                    service.startAsync().awaitRunning();
                }
                ExecutorService executor = getRuntimeModule().getExecutors().get(ExecutorService.class);
                SubmitIterator<Records.Request, Message.ServerResponse<?>> operations = SubmitIterator.create(
                         Iterators.transform(PerfectTreePaths.forParameters(getParameters()), 
                                 new Function<ZNodePath, Records.Request>() {
                                    @Override
                                    public Records.Request apply(
                                            ZNodePath path) {
                                        return Operations.Requests.create().setPath(path).build();
                                    }
                         }),
                         untraced.getConnectionClientExecutor());
                IteratingClient client = IteratingClient.create(executor, operations, LoggingPromise.create(logger, SettableFuturePromise.<Void>create()));
                executor.execute(client);
                try {
                    client.get();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                } finally {
                    for (Service service: Lists.reverse(untraced.build())) {
                        service.stopAsync().awaitTerminated();
                    }
                }
                delegate.run();
            }
        };
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected Generator<? extends Records.Request> getDefaultRequestGenerator() {
        final Configuration configuration = getRuntimeModule().getConfiguration();
        final Float getPercentage = GetPercentageConfiguration.get(configuration);
        final Float dataMaxMB = DataMaxMBConfiguration.get(configuration);
        final ImmutableList.Builder<ZNodePath> paths = ImmutableList.builder();
        paths.add(ZNodePath.root());
        paths.addAll(PerfectTreePaths.forParameters(parameters));
        return PathRequestGenerator.create(
                RandomFromList.create(getRandom(), paths.build()),
                Generators.dereferencing(
                        BinGenerator.create(
                            getRandom(), 
                            Iterators.forArray(
                                    Pair.create(
                                            getPercentage, 
                                            Generators.constant(Operations.Requests.getData())), 
                                    Pair.create(
                                            Float.valueOf(1.0f - getPercentage.floatValue()), 
                                            SetDataGenerator.forData(RandomData.create(getRandom(), 0, (int) (dataMaxMB.floatValue() * Math.pow(2, 20)))))))));
    }
    
    protected Random getDefaultRandom() {
        return new Random();
    }
    
    protected PerfectTreeParameters getDefaultParameters() {
        final Configuration configuration = getRuntimeModule().getConfiguration();
        return PerfectTreeParameters.fromConfiguration(configuration);
    }
}
