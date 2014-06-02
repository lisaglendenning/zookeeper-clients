package edu.uw.zookeeper.client.trace.csv;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.trace.TraceEventIterator;
import edu.uw.zookeeper.client.trace.TraceEventTag;
import edu.uw.zookeeper.client.trace.TraceHeader;
import edu.uw.zookeeper.client.trace.csv.CsvSchema.CsvSchemaBuilder;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.RuntimeModule;

public class CsvExport implements Application {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new Builder());
    }

    protected final Logger logger;
    protected final Builder builder;
    protected final ImmutableList<Application> exporters;
    
    protected CsvExport(Builder builder) {
        this.logger = LogManager.getLogger(getClass());
        this.builder = builder;
        
        ImmutableList.Builder<Application> exporters = ImmutableList.builder();
        boolean latencies = LatencySeriesConfiguration.get(builder.getRuntimeModule().getConfiguration());
        boolean operations = OperationsSeriesConfiguration.get(builder.getRuntimeModule().getConfiguration());
        if (! builder.getRuntimeModule().getConfiguration().getArguments().helpOptionSet()) {
            try {
                TraceEventIterator events = TraceEventIterator.forFile(builder.getInputFile(), builder.getObjectMapper().reader());
                TraceHeader header = events.header();
                events.close();
                if (! header.getTypes().contains(TraceEventTag.LATENCY_MEASUREMENT_EVENT)) {
                    latencies = false;
                }
                if (! header.getTypes().contains(TraceEventTag.THROUGHPUT_MEASUREMENT_EVENT)) {
                    operations = false;
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        if (latencies) {
            exporters.add(new ExportLatencySeries());
        }
        if (operations) {
            exporters.add(new ExportOperationsSeries());
        }
        this.exporters = exporters.build();
    }

    @Override
    public void run() {
        for (Application e: exporters) {
            e.run();
        }
    }
    
    public static class Builder extends CsvExportBuilder<CsvExport, Builder> {

        public Builder() {
            this(null, null, null, null, null);
        }
        
        protected Builder(File inputPath, File outputPath,
                CsvSchemaBuilder schema, ObjectMapper mapper,
                RuntimeModule runtime) {
            super(inputPath, outputPath, schema, mapper, runtime);
        }

        @Override
        protected Builder newInstance(File inputFile, File outputFile,
                CsvSchemaBuilder schema, ObjectMapper mapper,
                RuntimeModule runtime) {
            return new Builder(inputFile, outputFile, schema, mapper, runtime);
        }

        @Override
        protected CsvExport doBuild() {
            return new CsvExport(this);
        }
    }

    @Configurable(arg="latencies", key="latencySeries", value="true", type=ConfigValueType.BOOLEAN)
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

    @Configurable(arg="operations", key="operationsSeries", value="true", type=ConfigValueType.BOOLEAN)
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


    public class ExportOperationsSeries implements Application {

        @Override
        public void run() {
            File inputPath = builder.getInputFile();
            String filePrefix = inputPath.getName().substring(
                    0, inputPath.getName().lastIndexOf('.'));
            try {
                TraceEventIterator events = TraceEventIterator.forFile(inputPath, builder.getObjectMapper().reader());
                OperationsTimeSeries.eventsToCsvFile(
                        builder.getCsvSchema(), 
                        OperationsTimeSeries.toFile(builder.getOutputFile(), filePrefix), 
                        events);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    public class ExportLatencySeries implements Application {

        @Override
        public void run() {
            File inputPath = builder.getInputFile();
            String filePrefix = inputPath.getName().substring(
                    0, inputPath.getName().lastIndexOf('.'));
            try {
                TraceEventIterator events = TraceEventIterator.forFile(inputPath, builder.getObjectMapper().reader());
                LatencySeries.eventsToCsvFile(
                        builder.getCsvSchema(), 
                        LatencySeries.toFile(builder.getOutputFile(), filePrefix), 
                        events);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
