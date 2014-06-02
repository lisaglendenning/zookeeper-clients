package edu.uw.zookeeper.client.trace;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;
import edu.uw.zookeeper.common.Builder;

public class TraceWriterBuilder implements Builder<TraceWriter> {

    public static TraceWriterBuilder defaults() {
        return new TraceWriterBuilder();
    }
    
    protected final ObjectWriter writer;
    protected final Executor executor;
    protected final File file;
    protected final TraceHeader header;
    
    public TraceWriterBuilder() {
        this(null, null, null, null);
    }
    
    public TraceWriterBuilder(
            ObjectWriter writer,
            Executor executor,
            File file,
            TraceHeader header) {
        this.writer = writer;
        this.executor = executor;
        this.file = file;
        this.header = header;
    }
    
    public ObjectWriter getWriter() {
        return writer;
    }
    
    public TraceWriterBuilder setWriter(ObjectWriter writer) {
        return new TraceWriterBuilder(writer, executor, file, header);
    }

    public Executor getExecutor() {
        return executor;
    }

    public TraceWriterBuilder setExecutor(Executor executor) {
        return new TraceWriterBuilder(writer, executor, file, header);
    }

    public File getFile() {
        return file;
    }

    public TraceWriterBuilder setFile(File file) {
        return new TraceWriterBuilder(writer, executor, file, header);
    }

    public TraceHeader getHeader() {
        return header;
    }

    public TraceWriterBuilder setHeader(TraceHeader header) {
        return new TraceWriterBuilder(writer, executor, file, header);
    }

    @Override
    public TraceWriter build() {
        try {
            return TraceWriter.forFile(
                    file, 
                    writer,
                    header,
                    executor);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
