package edu.uw.zookeeper.client.trace.csv;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.client.trace.LatencyMeasurementEvent;
import edu.uw.zookeeper.client.trace.TraceEvent;

public class LatencySeries {
    
    public static String FILENAME_FORMAT = "%s-latencies.%s";

    public static File toFile(File parent, String prefix) {
        return new File(parent, String.format(FILENAME_FORMAT, prefix, Csv.SUFFIX));
    }
    
    public static void eventsToCsvFile(
            CsvSchema.CsvSchemaBuilder schema, File output, Iterator<TraceEvent> events) throws IOException {
        CsvAppender writer = CsvAppender.forFile(
                output, 
                schema.withColumns(columns()).build());
        Iterator<ImmutableList<Float>> records = fromEvents(events);
        while (records.hasNext()) {
            writer.append(records.next());
        }
        writer.flush();
        writer.close();
    }

    public static ImmutableList<CsvColumn> columns() {
        return ImmutableList.of(CsvColumn.create(
                CsvFieldType.FIELD_FLOAT, "Latency (ms)", CsvSchema.forFormat("%04.3f")));
    }
    
    public static Iterator<ImmutableList<Float>> fromEvents(Iterator<TraceEvent> events) {
        return toRecords(
                bound(
                        Iterators.filter(
                                events, LatencyMeasurementEvent.class)));
    }
    
    public static Iterator<LatencyMeasurementEvent> bound(
            Iterator<LatencyMeasurementEvent> events) {
        return Iterators.filter(
                events,
                new Predicate<LatencyMeasurementEvent>() {
                    @Override
                    public boolean apply(LatencyMeasurementEvent input) {
                        return input.getMicros() >= 0;
                    }
                });
    }
    
    public static Iterator<ImmutableList<Float>> toRecords(Iterator<LatencyMeasurementEvent> events) {
        return Iterators.transform(
                events,
                new Function<LatencyMeasurementEvent, ImmutableList<Float>>() {
                    @Override
                    public ImmutableList<Float> apply(LatencyMeasurementEvent input) {
                        return ImmutableList.of(Float.valueOf(input.getMicros() / 1000.0f));
                    }
                });
    }
}
