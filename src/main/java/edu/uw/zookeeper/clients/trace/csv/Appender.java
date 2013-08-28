package edu.uw.zookeeper.clients.trace.csv;

import java.io.IOException;

public interface Appender<T> {
    Appender<T> append(T value) throws IOException;
}