package edu.uw.zookeeper.clients.trace.csv;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import edu.uw.zookeeper.common.Pair;

public class CsvWriter implements Closeable, Flushable, Appender<Iterable<?>> {

    public static CsvWriter forFile(
            File file,
            CsvSchema schema) throws IOException {
        return forStream(new FileOutputStream(file), schema);
    }

    public static CsvWriter forStream(
            OutputStream stream,
            CsvSchema schema) throws IOException {
        return forWriter(new OutputStreamWriter(stream, Csv.CHARSET), schema);
    }

    public static CsvWriter forWriter(
            Writer writer,
            CsvSchema schema) throws IOException {
        return new CsvWriter(writer, schema);
    }
    
    protected final Writer writer;
    protected final CsvSchema schema;
    
    protected CsvWriter(
            Writer writer,
            CsvSchema schema) throws IOException {
        this.schema = checkNotNull(schema);
        this.writer = checkNotNull(writer);
        
        schema.append(writer);
    }
    
    @Override
    public CsvWriter append(Iterable<?> record) throws IOException {
        Iterator<Pair<CsvColumn, Object>> itr = ZippedIterator.zip(schema.getColumns().iterator(), record.iterator());
        while (itr.hasNext()) {
            Pair<CsvColumn, Object> next = itr.next();
            CsvColumn column = next.first();
            Object field = next.second();
            if (column == null) {
                throw new IllegalArgumentException(String.valueOf(record));
            }
            writer.write(column.getFormat().apply(field));
            if (itr.hasNext()) {
                schema.getDelimitColumn().append(writer);
            }
        }
        schema.getDelimitRecord().append(writer);
        return this;
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
