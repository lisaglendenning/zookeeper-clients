package edu.uw.zookeeper.clients.trace;

import com.fasterxml.jackson.core.JsonEncoding;
import com.google.common.base.Function;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

public abstract class Trace {

    public static JsonEncoding ENCODING = JsonEncoding.UTF8;
    
    public static String getTraceOutputFileConfiguration(Configuration configuration) {
        return TraceOutputFileConfiguration.get(configuration);
    }

    public static String getTraceInputFileConfiguration(Configuration configuration) {
        return TraceInputFileConfiguration.get(configuration);
    }
    
    @Configurable(path="Trace", arg="output", key="OutputFile", value="trace-%d.json", help="Path")
    public static class TraceOutputFileConfiguration implements Function<Configuration, String> {

        public static String get(Configuration configuration) {
            return new TraceOutputFileConfiguration().apply(configuration);
        }

        @Override
        public String apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            String value =
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key());
            if (value.indexOf('%') != -1) {
                value = String.format(value, System.currentTimeMillis());
            }
            return value;
        }   
    }
    
    @Configurable(path="Trace", arg="input", key="InputFile", value="trace.json", help="Path")
    public static class TraceInputFileConfiguration implements Function<Configuration, String> {

        public static String get(Configuration configuration) {
            return new TraceInputFileConfiguration().apply(configuration);
        }

        @Override
        public String apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            String value =
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key());
            return value;
        }   
    }
}
