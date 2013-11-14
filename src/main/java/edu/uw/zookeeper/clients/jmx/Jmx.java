package edu.uw.zookeeper.clients.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.data.DefaultsZNodeLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;

public abstract class Jmx {
    
    /**
     * Takes a JVM id as a command-line argument.
     * Prints org.apache.ZooKeeperService MBeans.
     */
    public static void main(String[] args) throws IOException {
        DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
        JMXServiceURL url = (args.length > 0) ? urlFactory.get(args[0]) : urlFactory.get();
        JMXConnector connector = JMXConnectorFactory.connect(url);
        try {
            MBeanServerConnection mbeans = connector.getMBeanServerConnection();
            for (ServerSchema schema: ServerSchema.values()) {
                DefaultsZNodeLabelTrie<JmxBeanNode> objectNames = schema.instantiate(mbeans);
                if (! objectNames.isEmpty()) {
                    for (JmxBeanNode e: objectNames) {
                        System.out.printf("%s = %s%n", e.path(), e.getNames());
                    }
                }
            }
        } finally {
            connector.close();
        }
    }

    public static String FORMAT_REGEX = "%.";
    public static char WILDCARD = '*';
    
    public static String patternOf(String format) {
        return format.toString().replaceAll(FORMAT_REGEX, Character.toString(WILDCARD));
    }

    public static String listPatternOf(String name) {
        return name + ",*";
    }

    public abstract static class PathObjectName {

        public static char KEY_SEPARATOR = '=';
        public static char PROPERTY_SEPARATOR = ',';
        
        public static ObjectName of(ZNodeLabel.Path input) {
            return PathToObjectName.ZOOKEEPER_SERVICE.apply(input);
        }
        
        public static ZNodeLabel.Path of(ObjectName input) {
            return ObjectNameToPath.ZOOKEEPER_SERVICE.apply(input);
        }
        
        public static enum PathToObjectName implements Function<ZNodeLabel.Path, ObjectName> {
            ZOOKEEPER_SERVICE(Domain.ZOOKEEPER_SERVICE);
        
            public static String KEY_FORMAT = "name%d";
            public static String PROPERTY_FORMAT = KEY_FORMAT + KEY_SEPARATOR + "%s";
            public static Joiner JOINER = Joiner.on(PROPERTY_SEPARATOR).useForNull("*");
            
            private final Domain domain;
            
            private PathToObjectName(Domain domain) {
                this.domain = domain;
            }
            
            @Override
            public ObjectName apply(ZNodeLabel.Path input) {
                List<String> properties = Lists.newLinkedList();
                int index = 0;
                for (ZNodeLabel.Component component: input) {
                    properties.add(String.format(PROPERTY_FORMAT, index, component.toString()));
                    index += 1;
                }
                return domain.apply(JOINER.join(properties));
            }
        }
        
        public static enum ObjectNameToPath implements Function<ObjectName, ZNodeLabel.Path> {
            ZOOKEEPER_SERVICE;

            public static Splitter PROPERTY_SPLITTER = Splitter.on(PROPERTY_SEPARATOR).omitEmptyStrings();
            public static Splitter KEY_SPLITTER = Splitter.on(KEY_SEPARATOR);
            
            @Override
            public ZNodeLabel.Path apply(ObjectName input) {
                List<ZNodeLabel> components = Lists.newLinkedList();
                components.add(ZNodeLabel.Path.root());
                for (String property: PROPERTY_SPLITTER.split(input.getCanonicalKeyPropertyListString())) {
                    String component = Iterables.toArray(KEY_SPLITTER.split(property), String.class)[1];
                    components.add(ZNodeLabel.Component.of(component));
                }
                return (ZNodeLabel.Path) ZNodeLabel.joined(components.iterator());
            }
        }
    }

    public static enum Domain implements Function<String, ObjectName> {
        ZOOKEEPER_SERVICE("org.apache.ZooKeeperService"), LOG4J("log4j");
        
        public static char DOMAIN_SEPARATOR = ':';
        public static final Joiner JOINER = Joiner.on(DOMAIN_SEPARATOR);
        
        private final String value;
        
        private Domain(String value) {
            this.value = value;
        }
        
        public String value() {
            return value;
        }

        @Override
        public ObjectName apply(String input) {
            String[] parts = {value(), input};
            try {
                return ObjectName.getInstance(JOINER.join(parts));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    public static enum Key {
        STANDALONE_SERVER("StandaloneServer_port%d"),
        REPLICATED_SERVER("ReplicatedServer_id%d"),
        REPLICA("replica.%d"),
        LEADER("Leader"), 
        FOLLOWER("Follower"),
        LEADER_ELECTION("LeaderElection"),
        IN_MEMORY_DATA_TREE("InMemoryDataTree");
        
        private final String value;
        
        private Key(String value) {
            this.value = value;
        }
        
        public ZNodeLabel.Component label() {
            return ZNodeLabel.Component.of(value);
        }

        @Override
        public String toString() {
            return value;
        }
    }
    
    public static class JmxSchemaNode extends DefaultsZNodeLabelTrie.AbstractDefaultsNode<JmxSchemaNode> {

        public static JmxSchemaNode root() {
            return new JmxSchemaNode(ZNodeLabelTrie.<JmxSchemaNode>strongPointer(ZNodeLabel.none(), null));
        }
        
        protected JmxSchemaNode(
                ZNodeLabelTrie.Pointer<? extends JmxSchemaNode> parent) {
            super(ZNodeLabelTrie.pathOf(parent), parent, Maps.<ZNodeLabel.Component, JmxSchemaNode>newHashMap());
        }

        @Override
        protected JmxSchemaNode newChild(ZNodeLabel.Component label) {
            return new JmxSchemaNode(ZNodeLabelTrie.weakPointer(label, this));
        }
    }
    
    public static class JmxBeanNode extends DefaultsZNodeLabelTrie.AbstractDefaultsNode<JmxBeanNode> {

        public static JmxBeanNode root() {
            return new JmxBeanNode(
                    ImmutableSet.<ObjectName>of(),
                    ZNodeLabelTrie.<JmxBeanNode>strongPointer(ZNodeLabel.none(), null));
        }

        public static JmxBeanNode child(
                ZNodeLabel.Component label, JmxBeanNode parent) {
            return child(
                    ImmutableSet.<ObjectName>of(),
                    label,
                    parent);
        }

        public static JmxBeanNode child(
                Set<ObjectName> names,
                ZNodeLabel.Component label,
                JmxBeanNode parent) {
            return new JmxBeanNode(
                    ImmutableSet.copyOf(names),
                    ZNodeLabelTrie.<JmxBeanNode>weakPointer(label, parent));
        }
        
        protected volatile ImmutableSet<ObjectName> names;
        
        protected JmxBeanNode(
                ImmutableSet<ObjectName> names,
                ZNodeLabelTrie.Pointer<? extends JmxBeanNode> parent) {
            super(ZNodeLabelTrie.pathOf(parent), parent, Maps.<ZNodeLabel.Component, JmxBeanNode>newHashMap());
            this.names = names;
        }
        
        public ImmutableSet<ObjectName> getNames() {
            return names;
        }

        public void setNames(Set<ObjectName> names) {
            this.names = ImmutableSet.copyOf(names);
        }
        
        public synchronized JmxBeanNode putIfAbsent(ZNodeLabel.Component label, Set<ObjectName> names) {
            JmxBeanNode child = delegate().get(label);
            if (child != null) {
                child.setNames(names);
            } else {
                child = child(names, label, this);
                delegate().put(label, child);
            }
            return child;
        }
        
        @Override
        protected JmxBeanNode newChild(ZNodeLabel.Component label) {
            return child(label, this);
        }
    }
    
    public static enum ServerSchema {
        STANDALONE_SERVER(Key.STANDALONE_SERVER),
        REPLICATED_SERVER(Key.REPLICATED_SERVER);
        
        private final DefaultsZNodeLabelTrie<JmxSchemaNode> trie;
        
        private ServerSchema(Key rootKey) {
            this.trie = DefaultsZNodeLabelTrie.of(JmxSchemaNode.root());
            JmxSchemaNode root = this.trie.root().putIfAbsent(rootKey.label());
            
            switch (rootKey) {
            case STANDALONE_SERVER:
            {
                root.putIfAbsent(Key.IN_MEMORY_DATA_TREE.label());
                break;
            }
            case REPLICATED_SERVER:
            {
                JmxSchemaNode replica = root.putIfAbsent(Key.REPLICA.label());
                Key[] roles = { Key.FOLLOWER, Key.LEADER, Key.LEADER_ELECTION };
                for (Key k: roles) {
                    JmxSchemaNode role = replica.putIfAbsent(k.label());
                    if (k != Key.LEADER_ELECTION) {
                        role.putIfAbsent(Key.IN_MEMORY_DATA_TREE.label());
                    }
                }
                break;
            }
            default:
                throw new AssertionError();
            }
        }
        
        public DefaultsZNodeLabelTrie<JmxSchemaNode> asTrie() {
            return trie;
        }
        
        public ZNodeLabel.Path pathOf(Key key) {
            for (JmxSchemaNode n: asTrie()) {
                ZNodeLabel.Path path = n.path();
                if (path.isRoot()) {
                    continue;
                }
                if (path.tail().toString().equals(key.toString())) {
                    return path;
                }
            }
            throw new IllegalArgumentException(key.toString());
        }
        
        public DefaultsZNodeLabelTrie<JmxBeanNode> instantiate(MBeanServerConnection mbeans) throws IOException {
            DefaultsZNodeLabelTrie<JmxBeanNode> instance = DefaultsZNodeLabelTrie.of(JmxBeanNode.root());
            for (JmxSchemaNode n: asTrie()) {
                ZNodeLabel.Path path = n.path();
                if (path.isRoot()) {
                    continue;
                }
                instance.putIfAbsent(path.head(), new InstantiateVisitor(n, mbeans));
            }
            
            return instance;
        }
    }
    
    public static class InstantiateVisitor implements Function<JmxBeanNode, Void> {
        
        private final JmxSchemaNode schema;
        private final MBeanServerConnection mbeans;
        
        public InstantiateVisitor(JmxSchemaNode schema, MBeanServerConnection mbeans) {
            this.schema = schema;
            this.mbeans = mbeans;
        }
        
        @Override
        public Void apply(JmxBeanNode parent) {
            try {
                ZNodeLabel.Path path = schema.path();
                Set<ObjectName> names;
                if (path.toString().indexOf('%') >= 0) {
                    // convert format to pattern
                    ObjectName pattern = PathObjectName.of(ZNodeLabel.Path.of(patternOf(path.toString())));
                    names = mbeans.queryNames(pattern, null);
                } else {
                    ObjectName result = PathObjectName.of(path);
                    if (mbeans.isRegistered(result)) {
                        names = ImmutableSet.of(result);
                    } else {
                        names = ImmutableSet.of();
                    }
                }
                parent.putIfAbsent((ZNodeLabel.Component) schema.parent().label(), names);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return null;
        }
    }
    
    public static enum PlatformMBeanServerFactory implements Factory<MBeanServer> {
        PLATFORM;
        
        public static PlatformMBeanServerFactory getInstance() {
            return PLATFORM;
        }

        @Override
        public MBeanServer get() {
            return ManagementFactory.getPlatformMBeanServer();
        }
    }

    private Jmx() {}
}
