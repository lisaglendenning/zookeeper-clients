package edu.uw.zookeeper.client.trace.csv;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;

import com.google.common.base.Objects;

public class StringDelimiter implements Delimiter {

    public static StringDelimiter forString(String value) {
        return new StringDelimiter(value);
    }
    
    protected final String value;

    public StringDelimiter(String value) {
        this.value = checkNotNull(value);
    }
    
    @Override
    public StringDelimiter append(Writer writer) throws IOException {
        writer.write(value);
        return this;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != getClass())) {
            return false;
        }
        StringDelimiter other = (StringDelimiter) obj;
        return Objects.equal(value, other.value);
    }
}
