package edu.uw.zookeeper.client.trace.csv;

import com.google.common.base.Function;
import com.google.common.base.Objects;

public final class CsvColumn {
    
    public static CsvColumn defaults() {
        return unnamed(CsvFieldType.FIELD_STRING);
    }

    public static CsvColumn unnamed(CsvFieldType type) {
        return create(type, "", CsvSchema.toStringFormatter());
    }

    public static CsvColumn named(CsvFieldType type, String name) {
        return create(type, name, CsvSchema.toStringFormatter());
    }

    public static CsvColumn create(CsvFieldType type, String name,
            Function<Object, String> format) {
        return new CsvColumn(type, name, format);
    }
    
    private final CsvFieldType type;
    private final String name;
    private final Function<Object, String> format;
    
    protected CsvColumn(CsvFieldType type, String name,
            Function<Object, String> format) {
        super();
        this.type = type;
        this.name = name;
        this.format = format;
    }

    public CsvFieldType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Function<Object, String> getFormat() {
        return format;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(name)
                .addValue(type)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof CsvColumn)) {
            return false;
        }
        CsvColumn other = (CsvColumn) obj;
        return Objects.equal(name, other.name)
                && Objects.equal(type, other.type)
                && Objects.equal(format, other.format);
    }
}
