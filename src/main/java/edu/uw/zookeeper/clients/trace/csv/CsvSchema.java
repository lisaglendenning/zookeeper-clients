package edu.uw.zookeeper.clients.trace.csv;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.common.Builder;


public class CsvSchema implements Appender<Writer> {

    public static CsvSchemaBuilder builder() {
        return CsvSchemaBuilder.defaults();
    }
    
    public static class CsvSchemaBuilder implements Builder<CsvSchema> {

        public static CsvSchemaBuilder defaults() {
            return new CsvSchemaBuilder(
                    ImmutableList.<CsvColumn>of(), 
                    Optional.of(
                            Functions.compose(
                                    QuoteString.quoteAll(),
                                    columnNameFormatter())), 
                    StringDelimiter.forString(COMMA), 
                    StringDelimiter.forString(NEWLINE));
        }
        
        public static final String NEWLINE = "\n";
        public static final String COMMA = ",";
        public static final String TAB = "\t";

        protected final ImmutableList<CsvColumn> columns;
        protected final Optional<? extends Function<? super CsvColumn, String>> columnFormatter;
        protected final Delimiter delimitColumn;
        protected final Delimiter delimitRecord;
        
        public CsvSchemaBuilder(
                ImmutableList<CsvColumn> columns,
                Optional<? extends Function<? super CsvColumn, String>> columnFormatter,
                Delimiter delimitColumn,
                Delimiter delimitRecord) {
            this.columns = columns;
            this.columnFormatter = columnFormatter;
            this.delimitColumn = delimitColumn;
            this.delimitRecord = delimitRecord;
        }

        public CsvSchemaBuilder withColumns(ImmutableList<CsvColumn> columns) {
            return newInstance(columns, columnFormatter, delimitColumn, delimitRecord);
        }

        public CsvSchemaBuilder delimitColumns(String delimiter) {
            return newInstance(columns, columnFormatter, StringDelimiter.forString(delimiter), delimitRecord);
        }
        
        public CsvSchemaBuilder delimitRecords(String delimiter) {
            return newInstance(columns, columnFormatter, delimitColumn, StringDelimiter.forString(delimiter));
        }
        
        public CsvSchemaBuilder formatColumns(Function<? super CsvColumn, String> formatter) {
            return newInstance(columns, Optional.<Function<? super CsvColumn, String>>of(formatter), delimitColumn, delimitRecord);
        }
        
        @Override
        public CsvSchema build() {
            return new CsvSchema(columns, columnFormatter, delimitColumn, delimitRecord);
        }
        
        protected CsvSchemaBuilder newInstance(
                ImmutableList<CsvColumn> columns,
                Optional<? extends Function<? super CsvColumn, String>> columnFormatter,
                Delimiter delimitColumn,
                Delimiter delimitRecord) {
            return new CsvSchemaBuilder(columns, columnFormatter, delimitColumn, delimitRecord);
        }
    }

    public static Function<Object, String> toStringFormatter() {
        return Functions.toStringFunction();
    }
    
    public static Function<Object, String> forFormat(String format) {
        return FormatStringFormatter.forFormat(format);
    }

    public static Function<CsvColumn, String> columnNameFormatter() {
        return new Function<CsvColumn, String>() {
            @Override
            public String apply(CsvColumn input) {
                return input.getName();
            }
        };
    }
    
    public static class FormatStringFormatter implements Function<Object, String> {

        public static FormatStringFormatter forFormat(String format) {
            return new FormatStringFormatter(format);
        }
        
        protected final String format;
        
        public FormatStringFormatter(String format) {
            this.format = format;
        }
        
        @Override
        public String apply(Object input) {
            return String.format(format, input);
        }
    }
    
    public static class QuoteString implements Function<String, String> {
        
        public static QuoteString quoteAll() {
            return new QuoteString();
        }
        
        @Override
        public String apply(String input) {
            return new StringBuilder()
                .append('"')
                .append(input)
                .append('"').toString();
        }
    }
    
    protected final ImmutableList<CsvColumn> columns;
    protected final Delimiter delimitColumn;
    protected final Delimiter delimitRecord;
    protected final Optional<? extends Function<? super CsvColumn, String>> columnFormatter;
    
    public CsvSchema(
            ImmutableList<CsvColumn> columns,
            Optional<? extends Function<? super CsvColumn, String>> columnFormatter,
            Delimiter delimitColumn,
            Delimiter delimitRecord) {
        this.columns = checkNotNull(columns);
        this.delimitColumn = checkNotNull(delimitColumn);
        this.columnFormatter = checkNotNull(columnFormatter);
        this.delimitRecord = checkNotNull(delimitRecord);
    }

    public ImmutableList<CsvColumn> getColumns() {
        return columns;
    }

    public Delimiter getDelimitColumn() {
        return delimitColumn;
    }

    public Delimiter getDelimitRecord() {
        return delimitRecord;
    }

    @Override
    public CsvSchema append(Writer writer) throws IOException {
        if (columnFormatter.isPresent()) {
            Iterator<CsvColumn> itr = columns.iterator();
            while (itr.hasNext()) {
                writer.write(columnFormatter.get().apply(itr.next()));
                if (itr.hasNext()) {
                    delimitColumn.append(writer);
                }
            }
            delimitRecord.append(writer);
        }
        return this;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(columns)
                .add("column", delimitColumn)
                .add("record", delimitRecord)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columns);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != getClass())) {
            return false;
        }
        CsvSchema other = (CsvSchema) obj;
        return Objects.equal(columns, other.columns)
                && Objects.equal(columnFormatter, other.columnFormatter)
                && Objects.equal(delimitColumn, other.delimitColumn)
                && Objects.equal(delimitRecord, other.delimitRecord);
    }
}
