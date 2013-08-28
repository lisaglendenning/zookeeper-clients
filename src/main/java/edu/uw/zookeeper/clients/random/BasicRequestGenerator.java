package edu.uw.zookeeper.clients.random;

import java.util.Random;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.clients.CachedPaths;
import edu.uw.zookeeper.clients.Generator;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Stats;

public class BasicRequestGenerator implements Generator<Records.Request> {

    public static BasicRequestGenerator create(
            ZNodeViewCache<?,?,?> cache) {
        Random random = new Random();
        RandomLabel labels = RandomLabel.create(random, 1, 9);
        RandomData datum = RandomData.create(random, 0, 1024);
        CachedPaths paths = CachedPaths.create(cache, random);
        ImmutableRandomFromList<OpCode> opcodes = ImmutableRandomFromList.create(random, BASIC_OPCODES);
        return new BasicRequestGenerator(
                random, opcodes, paths, labels, datum, cache);
    }
    
    protected static final ImmutableList<OpCode> BASIC_OPCODES = ImmutableList.of(
            OpCode.CREATE,
            OpCode.GET_CHILDREN,
            OpCode.DELETE, 
            OpCode.EXISTS, 
            OpCode.GET_DATA, 
            OpCode.SET_DATA, 
            OpCode.SYNC);
    
    protected final Random random;
    protected final ZNodeViewCache<?,?,?> cache;
    protected final Generator<OpCode> opcodes;
    protected final Generator<ZNodeLabel.Path> paths;
    protected final Generator<ZNodeLabel.Component> labels;
    protected final Generator<byte[]> datum;

    public BasicRequestGenerator(
            Random random, 
            Generator<OpCode> opcodes,
            Generator<ZNodeLabel.Path> paths,
            Generator<ZNodeLabel.Component> labels, 
            Generator<byte[]> datum, 
            ZNodeViewCache<?,?,?> client) {
        this.random = random;
        this.opcodes = opcodes;
        this.labels = labels;
        this.datum = datum;
        this.cache = client;
        this.paths = paths;
    }
    
    @Override
    public synchronized Records.Request next() {
        ZNodeLabel.Path path;
        do {
            path = paths.next();
        } while (ZNodeLabel.Path.zookeeper().prefixOf(path));
        ZNodeViewCache.NodeCache<?> node = cache.trie().get(path);
        while (node == null) {
            path = paths.next();
            node = cache.trie().get(path);
        }
        StampedReference<Records.StatGetter> statView = node.asView(ZNodeViewCache.View.STAT);
        Records.StatGetter stat = (statView == null) ? null : statView.get();
        int version = (stat == null) ? Stats.VERSION_ANY : stat.getStat().getVersion();
        OpCode opcode;
        while (true) {
            opcode = opcodes.next();
            if (opcode == OpCode.DELETE) {
                if (path.isRoot() || !cache.trie().get(path).isEmpty()) {
                    continue;
                }
            } else if ((opcode == OpCode.CREATE) || (opcode == OpCode.CREATE2)) {
                if ((stat == null) 
                        || (stat.getStat().getEphemeralOwner() != Stats.CreateStat.ephemeralOwnerNone())) {
                    continue;
                }
            }
            break;
        }
        Operations.Builder<? extends Records.Request> builder = Operations.Requests.fromOpCode(opcode);
        if (builder instanceof Operations.Requests.Create) {
            CreateMode mode = CreateMode.values()[random.nextInt(CreateMode.values().length)];
            ZNodeLabel.Component child = labels.next();
            while (node.containsKey(child)) {
                child = labels.next();
            }
            ((Operations.Requests.Create) builder).setPath(ZNodeLabel.Path.of(path, child)).setMode(mode).setData(datum.next());          
        } else {
            ((Operations.PathBuilder<?,?>) builder).setPath(path);

            if (builder instanceof Operations.Requests.VersionBuilder<?,?>) {
                ((Operations.Requests.VersionBuilder<?,?>) builder).setVersion(version);
            }
            if (builder instanceof Operations.Requests.WatchBuilder<?,?>) {
                ((Operations.Requests.WatchBuilder<?,?>) builder).setWatch(random.nextBoolean());
            }
            if (builder instanceof Operations.DataBuilder<?,?>) {
                ((Operations.DataBuilder<?,?>) builder).setData(datum.next());
            }
        }

        return builder.build();
    }
}
