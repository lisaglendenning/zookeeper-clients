package edu.uw.zookeeper.clients.trace.csv;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.clients.trace.JacksonModule;
import edu.uw.zookeeper.clients.trace.Trace;
import edu.uw.zookeeper.clients.trace.TraceEventIterator;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

public class Main extends AbstractModule {

    public static Injector create(String[] args) {
        return new Main(DefaultRuntimeModule.configuration(args)).getInjector();
    }
    
    public static void main(String[] args) {
        Injector injector = create(args);
        injector.getInstance(Application.class).run();
    }

    protected final Configuration configuration;
    protected final Injector injector;
    protected final Logger logger = LogManager.getLogger(getClass());
    
    public Main(Configuration configuration) {
        this.configuration = configuration;
        this.injector = createInjector();
    }
    
    public Injector getInjector() {
        return injector;
    }
    
    protected Injector createInjector() {
        return Guice.createInjector(JacksonModule.create(), this);
    }
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public Configuration getConfiguration() {
        return configuration;
    }

    @Provides @Singleton
    public CsvSchema.Builder getCsvSchema() {
        return CsvSchema.builder();
    }
    
    @Provides @Singleton
    public Application getApplication(
            final Configuration configuration,
            final MainApplication main) {
        return new Application() {
            @Override
            public void run() {
                DefaultMain.exitIfHelpSet(configuration.getArguments());
                main.run();
            }
        };
    }
    
    @Configurable(arg="output", key="OutputPath", help="Path")
    public static class OutputPathConfiguration implements Function<Configuration, File> {

        public static File get(Configuration configuration) {
            return new OutputPathConfiguration().apply(configuration);
        }

        @Override
        public File apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                return new File(config.getString(configurable.key()));
            } else {
                return new File("").getAbsoluteFile();
            }
        }
    }

    @Configurable(arg="latencies", key="LatencySeries", value="true", type=ConfigValueType.BOOLEAN)
    public static class LatencySeriesConfiguration implements Function<Configuration, Boolean> {

        public static Boolean get(Configuration configuration) {
            return new LatencySeriesConfiguration().apply(configuration);
        }

        @Override
        public Boolean apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getBoolean(configurable.key());
        }
    }

    @Configurable(arg="operations", key="OperationsSeries", value="true", type=ConfigValueType.BOOLEAN)
    public static class OperationsSeriesConfiguration implements Function<Configuration, Boolean> {

        public static Boolean get(Configuration configuration) {
            return new OperationsSeriesConfiguration().apply(configuration);
        }

        @Override
        public Boolean apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getBoolean(configurable.key());
        }
    }
    
    @Singleton
    public static class MainApplication implements Application {

        protected final Logger logger = LogManager.getLogger(getClass());
        protected final File inputPath;
        protected final File outputPath;
        protected final CsvSchema.Builder schema;
        protected final ObjectMapper mapper;
        protected final Configuration configuration;
        
        @Inject
        public MainApplication(
                Configuration configuration,
                CsvSchema.Builder schema,
                ObjectMapper mapper) {
            this.outputPath = OutputPathConfiguration.get(configuration);
            this.inputPath = Trace.getTraceInputFileConfiguration(configuration);
            this.mapper = mapper;
            this.schema = schema;
            this.configuration = configuration;
        }
        
        @Override
        public void run() {
            logger.info("Trace input: {}", inputPath);
            logger.info("Trace output: {}", outputPath);
            
            String filePrefix = inputPath.getName().substring(
                    0, inputPath.getName().lastIndexOf('.'));

            if (LatencySeriesConfiguration.get(configuration)) {
                try {
                    TraceEventIterator events = TraceEventIterator.forFile(inputPath, mapper.reader());
                    LatencySeries.eventsToCsvFile(
                            schema, 
                            LatencySeries.toFile(outputPath, filePrefix), 
                            events);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }

            if (OperationsSeriesConfiguration.get(configuration)) {
                try {
                    TraceEventIterator events = TraceEventIterator.forFile(inputPath, mapper.reader());
                    OperationsTimeSeries.eventsToCsvFile(
                            schema, 
                            OperationsTimeSeries.toFile(outputPath, filePrefix), 
                            events);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }
}
