package edu.uw.zookeeper.clients.random;

import java.util.Random;

import edu.uw.zookeeper.client.ZNodeCacheTrie;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;

public class PathedRequestGenerator implements Generator<Records.Request> {

    public static <E extends ZNodeCacheTrie.CachedNode<E>> PathedRequestGenerator fromCache(
            ZNodeCacheTrie<? extends E,?,?> cache) {
        Random random = new Random();
        CachedPaths<E> paths = CachedPaths.create(cache, random);
        return exists(paths);
    }

    public static PathedRequestGenerator exists(
            Generator<ZNodeLabel.Path> paths) {
        Operations.PathBuilder<? extends Records.Request,?> operation = Operations.Requests.exists().setPath(ZNodeLabel.Path.root()).setWatch(false);
        return new PathedRequestGenerator(
                operation, paths);
    }
    
    protected final Generator<ZNodeLabel.Path> paths;
    protected final Operations.PathBuilder<? extends Records.Request,?> operation;

    public PathedRequestGenerator(
            Operations.PathBuilder<? extends Records.Request,?> operation,
            Generator<ZNodeLabel.Path> paths) {
        this.operation = operation;
        this.paths = paths;
    }
    
    @Override
    public synchronized Records.Request next() {
        return operation.setPath(paths.next()).build();
    }
}
