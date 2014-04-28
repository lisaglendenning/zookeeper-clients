package edu.uw.zookeeper.clients.random;

import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;

public class PathRequestGenerator implements Generator<Records.Request> {

    public static PathRequestGenerator create(
            Generator<? extends ZNodeLabel.Path> paths,
            Generator<? extends Operations.PathBuilder<? extends Records.Request, ?>> operations) {
        return new PathRequestGenerator(paths, operations);
    }
    
    protected final Generator<? extends ZNodeLabel.Path> paths;
    protected final Generator<? extends Operations.PathBuilder<? extends Records.Request, ?>> operations;

    protected PathRequestGenerator(
            Generator<? extends ZNodeLabel.Path> paths,
            Generator<? extends Operations.PathBuilder<? extends Records.Request, ?>> operations) {
        this.operations = operations;
        this.paths = paths;
    }
    
    public Generator<? extends ZNodeLabel.Path> getPaths() {
        return paths;
    }
    
    public Generator<? extends Operations.PathBuilder<? extends Records.Request, ?>> getOperations() {
        return operations;
    }
    
    @Override
    public Records.Request next() {
        return getOperations().next().setPath(getPaths().next()).build();
    }
}
