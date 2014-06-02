package edu.uw.zookeeper.client.trace.csv;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.client.trace.ThroughputMeasurementEvent;
import edu.uw.zookeeper.client.trace.TraceEvent;

public class OperationsTimeSeries {
    
    public static String FILENAME_FORMAT = "%s-operations.%s";

    public static File toFile(File parent, String prefix) {
        return new File(parent, String.format(FILENAME_FORMAT, prefix, Csv.SUFFIX));
    }
    
    public static void eventsToCsvFile(
            CsvSchema.CsvSchemaBuilder schema, File output, Iterator<TraceEvent> events) throws IOException {
        CsvAppender writer = CsvAppender.forFile(
                output, 
                schema.withColumns(columns()).build());
        Iterator<ImmutableList<Integer>> records = fromEvents(events);
        while (records.hasNext()) {
            writer.append(records.next());
        }
        writer.flush();
        writer.close();
    }

    public static ImmutableList<CsvColumn> columns() {
        return ImmutableList.of(
                CsvColumn.create(
                        CsvFieldType.FIELD_INT, "Time (ms)", CsvSchema.forFormat("%d")),
                CsvColumn.create(
                        CsvFieldType.FIELD_INT, "Operations", CsvSchema.forFormat("%d")));
    }
    
    public static Iterator<ImmutableList<Integer>> fromEvents(Iterator<TraceEvent> events) {
        return toRecords(
                bound(
                        Iterators.filter(
                                events, ThroughputMeasurementEvent.class)));
    }
    
    public static Iterator<ThroughputMeasurementEvent> bound(
            Iterator<ThroughputMeasurementEvent> events) {
        return Iterators.filter(
                events,
                new Predicate<ThroughputMeasurementEvent>() {
                    @Override
                    public boolean apply(ThroughputMeasurementEvent input) {
                        return input.getCount() >= 0;
                    }
                });
    }
    
    public static Iterator<ImmutableList<Integer>> toRecords(Iterator<ThroughputMeasurementEvent> events) {
        return Iterators.transform(
                events,
                new Function<ThroughputMeasurementEvent, ImmutableList<Integer>>() {
                    int time = 0;
                    @Override
                    public ImmutableList<Integer> apply(ThroughputMeasurementEvent input) {
                        time += input.getMillis();
                        return ImmutableList.of(
                                Integer.valueOf(time), 
                                Integer.valueOf(input.getCount()));
                    }
                });
    }
}
