package edu.uw.zookeeper.clients.random;

import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.client.ZNodeViewCache.ViewUpdate;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public class CachedPaths implements Generator<ZNodeLabel.Path>, ZNodeViewCache.CacheSessionListener {

    public static CachedPaths create(ZNodeViewCache<?,?,?> cache, Random random) {
        CachedPaths instance = new CachedPaths(random, ImmutableList.<ZNodeLabel.Path>of());
        cache.subscribe(instance);
        synchronized (instance) {
            for (ZNodeViewCache.NodeCache<?> e: cache.trie()) {
                instance.add(e.path());
            }
        }
        return instance;
    }

    protected final Random random;
    protected final List<ZNodeLabel.Path> elements;
    
    public CachedPaths(Random random, Iterable<ZNodeLabel.Path> elements) {
        this.random = random;
        this.elements = Lists.newArrayList(elements);
    }

    public synchronized boolean add(ZNodeLabel.Path element) {
        if (! elements.contains(element)) {
            return elements.add(element);
        } else {
            return false;
        }
    }

    public synchronized boolean remove(ZNodeLabel.Path element) {
        return elements.remove(element);
    }

    @Override
    public synchronized ZNodeLabel.Path next() {
        int size = elements.size();
        if (size == 0) {
            return null;
        } else {
            int index = random.nextInt(size);
            return elements.get(index);
        }
    }

    @Override
    public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
        switch (event.type()) {
        case NODE_ADDED:
            add(event.path().get());
            break;
        case NODE_REMOVED:
            remove(event.path().get());
            break;
        }
    }

    @Override
    public void handleViewUpdate(ViewUpdate event) {
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
    }

    @Override
    public void handleNotification(Operation.ProtocolResponse<IWatcherEvent> notification) {
    }
}
