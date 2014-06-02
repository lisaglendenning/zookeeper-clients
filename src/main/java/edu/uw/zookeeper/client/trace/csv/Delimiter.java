package edu.uw.zookeeper.client.trace.csv;

import java.io.IOException;
import java.io.Writer;

public interface Delimiter {

    Delimiter append(Writer writer) throws IOException;
}
