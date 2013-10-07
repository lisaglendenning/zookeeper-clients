package edu.uw.zookeeper.clients.common;

import java.util.Iterator;

import com.google.common.collect.Iterators;



public abstract class Generators {
    
    public static <V> ConstantGenerator<V> constant(V value) {
        return new ConstantGenerator<V>(value);
    }

    public static <V> IteratorGenerator<V> cycle(Iterable<V> elements) {
        return IteratorGenerator.of(Iterators.cycle(elements));
    }

    public static <V> IteratorGenerator<V> cycle(V... elements) {
        return IteratorGenerator.of(Iterators.cycle(elements));
    }
    
    public static class ConstantGenerator<V> implements Generator<V> {

        public static <V> ConstantGenerator<V> of(V value) {
            return new ConstantGenerator<V>(value);
        }
        
        private final V value;
        
        public ConstantGenerator(V value) {
            this.value = value;
        }
        
        @Override
        public V next() {
            return value;
        }
    }

    public static class IteratorGenerator<V> implements Generator<V> {

        public static <V> IteratorGenerator<V> of(Iterator<V> value) {
            return new IteratorGenerator<V>(value);
        }
        
        private final Iterator<V> itr;

        protected IteratorGenerator(Iterator<V> itr) {
            this.itr = itr;
        }
        
        @Override
        public V next() {
            return itr.next();
        }
    }
    
    private Generators() {}
}
