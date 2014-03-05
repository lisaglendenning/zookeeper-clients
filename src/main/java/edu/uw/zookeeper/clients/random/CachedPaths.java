package edu.uw.zookeeper.clients.random;

import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.client.ZNodeCacheTrie;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public class CachedPaths<E extends ZNodeCacheTrie.CachedNode<E>> implements Generator<ZNodePath>, ZNodeCacheTrie.CacheSessionListener<E> {

    public static <E extends ZNodeCacheTrie.CachedNode<E>> CachedPaths<E> create(ZNodeCacheTrie<? extends E,?,?> cache, Random random) {
        CachedPaths<E> instance = new CachedPaths<E>(random, ImmutableList.<ZNodePath>of());
        cache.subscribe(instance);
        synchronized (instance) {
            for (ZNodeCacheTrie.CachedNode<?> e: cache) {
                instance.add(e.path());
            }
        }
        return instance;
    }

    protected final Random random;
    protected final List<ZNodePath> elements;
    
    public CachedPaths(Random random, Iterable<ZNodePath> elements) {
        this.random = random;
        this.elements = Lists.newArrayList(elements);
    }

    public synchronized boolean add(ZNodePath element) {
        if (! elements.contains(element)) {
            return elements.add(element);
        } else {
            return false;
        }
    }

    public synchronized boolean remove(ZNodePath element) {
        return elements.remove(element);
    }

    @Override
    public synchronized ZNodePath next() {
        int size = elements.size();
        if (size == 0) {
            return null;
        } else {
            int index = random.nextInt(size);
            return elements.get(index);
        }
    }

    @Override
    public void handleCacheUpdate(ZNodeCacheTrie.CacheEvent<? extends E> event) {
        if (event instanceof ZNodeCacheTrie.NodeAddedCacheEvent) {
            add(event.getNode().path());
        } else if (event instanceof ZNodeCacheTrie.NodeRemovedCacheEvent) {
            remove(event.getNode().path());
        }
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
    }

    @Override
    public void handleNotification(Operation.ProtocolResponse<IWatcherEvent> notification) {
    }
}
