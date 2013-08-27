package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Function;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(path="trace", arg="tracefile", key="File", value="trace-%d.json", help="Path")
public class TraceFileConfiguration implements Function<Configuration, String> {

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