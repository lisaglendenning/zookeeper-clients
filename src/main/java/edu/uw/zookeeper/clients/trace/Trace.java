package edu.uw.zookeeper.clients.trace;

import com.fasterxml.jackson.core.JsonEncoding;
import com.google.common.base.Function;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

public abstract class Trace {

    public static JsonEncoding ENCODING = JsonEncoding.UTF8;
    
    public static String getTraceFileConfiguration(Configuration configuration) {
        return TraceFileConfiguration.get(configuration);
    }
    
    @Configurable(path="trace", arg="tracefile", key="File", value="trace-%d.json", help="Path")
    public static class TraceFileConfiguration implements Function<Configuration, String> {

        public static String get(Configuration configuration) {
            return new TraceFileConfiguration().apply(configuration);
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
}
