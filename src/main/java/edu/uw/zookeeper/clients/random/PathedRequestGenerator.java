package edu.uw.zookeeper.clients.random;

import java.util.Random;

import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;

public class PathedRequestGenerator implements Generator<Records.Request> {

    public static PathedRequestGenerator fromCache(
            LockableZNodeCache<?,?,?> cache) {
        Random random = new Random();
        CachedPaths paths = CachedPaths.fromCache(cache, random);
        return exists(paths);
    }

    public static PathedRequestGenerator exists(
            Generator<? extends ZNodePath> paths) {
        Operations.PathBuilder<? extends Records.Request,?> operation = Operations.Requests.exists().setPath(ZNodePath.root()).setWatch(false);
        return new PathedRequestGenerator(
                operation, paths);
    }
    
    protected final Generator<? extends ZNodePath> paths;
    protected final Operations.PathBuilder<? extends Records.Request,?> operation;

    public PathedRequestGenerator(
            Operations.PathBuilder<? extends Records.Request,?> operation,
            Generator<? extends ZNodePath> paths) {
        this.operation = operation;
        this.paths = paths;
    }
    
    @Override
    public synchronized Records.Request next() {
        return operation.setPath(paths.next()).build();
    }
}
