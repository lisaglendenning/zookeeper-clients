package edu.uw.zookeeper.client.trace.csv;

import java.nio.charset.Charset;

import org.apache.logging.log4j.core.helpers.Charsets;

public abstract class Csv {
    public static final Charset CHARSET = Charsets.UTF_8;
    public static final String SUFFIX = "csv";
}
