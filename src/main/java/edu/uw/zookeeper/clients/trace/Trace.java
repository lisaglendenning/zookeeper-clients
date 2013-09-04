package edu.uw.zookeeper.clients.trace;

import java.io.File;

import com.fasterxml.jackson.core.JsonEncoding;
import com.google.common.base.Function;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

public abstract class Trace {

    public static JsonEncoding ENCODING = JsonEncoding.UTF8;
    
    public static File getTraceOutputFileConfiguration(Configuration configuration) {
        return TraceOutputFileConfiguration.get(configuration);
    }

    public static File getTraceInputFileConfiguration(Configuration configuration) {
        return TraceInputFileConfiguration.get(configuration);
    }
    
    public static String getTraceDescription(Configuration configuration) {
        return TraceDescriptionConfiguration.get(configuration);
    }
    
    @Configurable(path="Trace", arg="output", key="OutputFile", value="trace-%d.json", help="Path")
    public static class TraceOutputFileConfiguration implements Function<Configuration, File> {

        public static File get(Configuration configuration) {
            return new TraceOutputFileConfiguration().apply(configuration);
        }

        @Override
        public File apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            String value =
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key());
            if (value.indexOf('%') != -1) {
                value = String.format(value, System.currentTimeMillis());
            }
            return new File(value).getAbsoluteFile();
        }   
    }
    
    @Configurable(path="Trace", arg="input", key="InputFile", value="trace.json", help="Path")
    public static class TraceInputFileConfiguration implements Function<Configuration, File> {

        public static File get(Configuration configuration) {
            return new TraceInputFileConfiguration().apply(configuration);
        }

        @Override
        public File apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            String value =
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key());
            return new File(value).getAbsoluteFile();
        }   
    }
    
    @Configurable(arg="description", path="Trace", key="Description", help="Description", type=ConfigValueType.STRING)
    public static class TraceDescriptionConfiguration implements Function<Configuration, String> {

        public static String get(Configuration configuration) {
            return new TraceDescriptionConfiguration().apply(configuration);
        }

        @Override
        public String apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                return config.getString(configurable.key());
            } else {
                return "";
            }
        }
    }
}
