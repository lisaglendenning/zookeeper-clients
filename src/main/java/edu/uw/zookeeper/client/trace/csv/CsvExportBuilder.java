package edu.uw.zookeeper.client.trace.csv;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.typesafe.config.Config;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.trace.ObjectMapperBuilder;
import edu.uw.zookeeper.client.trace.Tracing;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.RuntimeModule;

public abstract class CsvExportBuilder<T, C extends CsvExportBuilder<T,C>> implements ZooKeeperApplication.RuntimeBuilder<T,C> {

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final RuntimeModule runtime;
    protected final File inputFile;
    protected final File outputFile;
    protected final CsvSchema.CsvSchemaBuilder schema;
    protected final ObjectMapper mapper;

    protected CsvExportBuilder(
            File inputPath,
            File outputPath,
            CsvSchema.CsvSchemaBuilder schema,
            ObjectMapper mapper,
            RuntimeModule runtime) {
        this.inputFile = inputPath;
        this.outputFile = outputPath;
        this.mapper = mapper;
        this.schema = schema;
        this.runtime = runtime;
    }
    
    @Override
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }
    
    @Override
    public C setRuntimeModule(RuntimeModule runtime) {
        return newInstance(inputFile, outputFile, schema, mapper, runtime);
    }

    public File getInputFile() {
        return inputFile;
    }

    public C setInputFile(File inputFile) {
        return newInstance(inputFile, outputFile, schema, mapper, runtime);
    }

    public File getOutputFile() {
        return outputFile;
    }

    public C setOutputFile(File outputFile) {
        return newInstance(inputFile, outputFile, schema, mapper, runtime);
    }

    public CsvSchema.CsvSchemaBuilder getCsvSchema() {
        return schema;
    }

    public C setCsvSchema(CsvSchema.CsvSchemaBuilder schema) {
        return newInstance(inputFile, outputFile, schema, mapper, runtime);
    }

    public ObjectMapper getObjectMapper() {
        return mapper;
    }

    public C setObjectMapper(ObjectMapper mapper) {
        return newInstance(inputFile, outputFile, schema, mapper, runtime);
    }

    @SuppressWarnings("unchecked")
    @Override
    public C setDefaults() {
        checkState(getRuntimeModule() != null);
        
        if (inputFile == null) {
            return setInputFile(getDefaultInputFile()).setDefaults();
        }
        if (outputFile == null) {
            return setOutputFile(getDefaultOutputFile()).setDefaults();
        }
        if (mapper == null) {
            return setObjectMapper(getDefaultObjectMapper()).setDefaults();
        }
        if (schema == null) {
            return setCsvSchema(getDefaultCsvSchema()).setDefaults();
        }
        return (C) this;
    }
    
    @Override
    public T build() {
        return setDefaults().doBuild();
    }
    
    protected abstract C newInstance(
            File inputFile,
            File outputFile,
            CsvSchema.CsvSchemaBuilder schema,
            ObjectMapper mapper,
            RuntimeModule runtime);

    protected abstract T doBuild();

    protected File getDefaultInputFile() {
        File file;
        try {
            file = Tracing.getTraceInputFileConfiguration(getRuntimeModule().getConfiguration()).getCanonicalFile();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        logger.info("Trace input file: {}", file);
        return file;
    }

    protected File getDefaultOutputFile() {
        File file;
        try {
            file = OutputPathConfiguration.get(getRuntimeModule().getConfiguration()).getCanonicalFile();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        logger.info("CSV output file: {}", file);
        return file;
    }
    
    protected CsvSchema.CsvSchemaBuilder getDefaultCsvSchema() {
        return CsvSchema.builder();
    }
    
    protected ObjectMapper getDefaultObjectMapper() {
        return ObjectMapperBuilder.defaults().build();
    }
    
    @Configurable(arg="output", path="csv", key="outputPath", help="path")
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
}