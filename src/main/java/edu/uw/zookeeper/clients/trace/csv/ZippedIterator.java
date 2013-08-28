package edu.uw.zookeeper.clients.trace.csv;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import edu.uw.zookeeper.common.Pair;

public class ZippedIterator<T,U> extends AbstractIterator<Pair<T,U>> {

    public static <T,U> ZippedIterator<T,U> zip(Iterator<? extends T> first, Iterator<? extends U> second) {
        return new ZippedIterator<T,U>(first, second);
    }
    
    private final Iterator<? extends T> first;
    private final Iterator<? extends U> second;
    
    public ZippedIterator(Iterator<? extends T> first, Iterator<? extends U> second) {
        this.first = first;
        this.second = second;
    }
    
    @Override
    protected Pair<T, U> computeNext() {
        T firstValue = first.hasNext() ? first.next() : null;
        U secondValue = second.hasNext() ? second.next() : null;
        if ((firstValue == null) && (secondValue == null)) {
            return endOfData();
        } else {
            return Pair.create(firstValue, secondValue);
        }
    }
}