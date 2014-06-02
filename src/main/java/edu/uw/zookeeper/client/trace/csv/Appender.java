package edu.uw.zookeeper.client.trace.csv;

import java.io.IOException;

public interface Appender<T> {
    Appender<T> append(T value) throws IOException;
}