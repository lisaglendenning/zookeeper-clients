package edu.uw.zookeeper.clients.trace;

import java.util.AbstractMap;
import java.util.Map;

import com.typesafe.config.Config;

import edu.uw.zookeeper.common.Arguments;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;

public class TraceFileConfiguration implements DefaultsFactory<Configuration, String> {

    public static TraceFileConfiguration newInstance() {
        return newInstance(
                DEFAULT_CONFIG_PATH, 
                DEFAULT_CONFIG_KEY, 
                DEFAULT_CONFIG_ARG, 
                String.format(DEFAULT_VALUE, System.currentTimeMillis()));
    }
            
    public static TraceFileConfiguration newInstance(
            String configPath, 
            String configKey, 
            String arg,
            String defaultValue) {
        return new TraceFileConfiguration(
                configPath, configKey, arg, defaultValue);
    }
    
    public static final String DEFAULT_CONFIG_PATH = "";
    public static final String DEFAULT_CONFIG_KEY = "TraceFile";
    public static final String DEFAULT_CONFIG_ARG = "tracefile";
    public static final String DEFAULT_VALUE = "trace-%d.json";

    protected final String configPath;
    protected final String configKey;
    protected final String arg;
    protected final String defaultValue;
    
    public TraceFileConfiguration(
            String configPath, String configKey, String arg, String defaultValue) {
        this.configPath = configPath;
        this.configKey = configKey;
        this.arg = arg;
        this.defaultValue = defaultValue;
    }
    
    @Override
    public String get() {
        return defaultValue;
    }

    @Override
    public String get(Configuration value) {
        Arguments arguments = value.asArguments();
        if (! arguments.has(arg)) {
            arguments.add(arguments.newOption(arg, "Path"));
        }
        arguments.parse();
        Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(arg, configKey);
        @SuppressWarnings("unchecked")
        Config config = value.withArguments(configPath, args);
        if (config.hasPath(configKey)) {
            return config.getString(configKey);
        } else {
            return get();
        }
    }   
}