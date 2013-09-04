package edu.uw.zookeeper.clients.random;

import edu.uw.zookeeper.clients.common.Generator;

public class ConstantGenerator<V> implements Generator<V> {

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
