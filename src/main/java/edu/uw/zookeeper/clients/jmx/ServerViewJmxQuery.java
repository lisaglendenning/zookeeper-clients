package edu.uw.zookeeper.clients.jmx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;

public abstract class ServerViewJmxQuery {

    /**
     * Takes a JVM id as a command-line argument.
     * Prints the server client network address if discovered.
     * Prints the ensemble quorum addresses if discovered.
     */
    public static void main(String[] args) throws Exception {        
        DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
        JMXServiceURL url = (args.length > 0) ? urlFactory.get(args[0]) : urlFactory.get();
        JMXConnector connector = JMXConnectorFactory.connect(url);
        StringBuilder output = new StringBuilder();
        try {
            MBeanServerConnection mbeans = connector.getMBeanServerConnection();
            output.append("ClientAddress").append(' ');
            ServerView.Address<InetSocketAddress> addressView = addressViewOf(mbeans);
            if (addressView != null) {
                output.append(addressView);
            } else {
                output.append("not found");
            }
            output.append('\n').append("Quorum").append(' ');
            EnsembleRoleView<InetSocketAddress, ServerInetAddressView> ensembleView = ensembleViewOf(mbeans);
            if (ensembleView != null) {
                output.append(ensembleView);
            } else {
                output.append("not found");
            }
            output.append('\n');
        } finally {
            connector.close();
        }
        System.out.println(output.toString());
    }

    public static final String CLIENT_PORT_ATTRIBUTE = "ClientPort";
    public static final String QUORUM_ADDRESS_ATTRIBUTE = "QuorumAddress";
    
    public static ServerInetAddressView addressViewOf(MBeanServerConnection mbeans) throws IOException {
        for (Jmx.ServerSchema schema: Jmx.ServerSchema.values()) {
            ZNodeLabelTrie<ZNodeLabelTrie.ValueNode<Set<ObjectName>>> objectNames = schema.instantiate(mbeans);
            if (objectNames == null || objectNames.isEmpty()) {
                continue;
            }

            switch (schema) {
            case STANDALONE_SERVER: 
            {
                ObjectName name = Iterables.getOnlyElement(objectNames.get(schema.pathOf(Jmx.Key.STANDALONE_SERVER)).get());
                String address;
                try {
                    address = (String) mbeans.getAttribute(name, CLIENT_PORT_ATTRIBUTE);
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                ServerInetAddressView addressView = ServerInetAddressView.fromString(address);
                return addressView;
            }
            case REPLICATED_SERVER:
            {
                Jmx.Key[] roles = { Jmx.Key.FOLLOWER, Jmx.Key.LEADER };
                for (Jmx.Key role: roles) {
                    ZNodeLabelTrie.ValueNode<Set<ObjectName>> node = objectNames.get(schema.pathOf(role));
                    if (node != null) {
                        ObjectName name = Iterables.getOnlyElement(node.get());
                        String address;
                        try {
                            address = (String) mbeans.getAttribute(name, CLIENT_PORT_ATTRIBUTE);
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        }
                        ServerInetAddressView addressView = ServerInetAddressView.fromString(address);
                        return addressView;
                    }
                }
            }
            default:
                throw new AssertionError();
            }
        }
        return null;
    }
    
    public static EnsembleRoleView<InetSocketAddress, ServerInetAddressView> ensembleViewOf(MBeanServerConnection mbeans) throws IOException {
        Jmx.ServerSchema schema = Jmx.ServerSchema.REPLICATED_SERVER;
        ZNodeLabelTrie<ZNodeLabelTrie.ValueNode<Set<ObjectName>>> objectNames = schema.instantiate(mbeans);
        if (objectNames == null || objectNames.isEmpty()) {
            return null;
        }

        Map<EnsembleRole, ZNodeLabel.Path> roles = 
                ImmutableMap.of(
                        EnsembleRole.LOOKING,
                        schema.pathOf(Jmx.Key.LEADER_ELECTION),
                        EnsembleRole.LEADING,
                        schema.pathOf(Jmx.Key.LEADER),
                        EnsembleRole.FOLLOWING,
                        schema.pathOf(Jmx.Key.FOLLOWER));
        List<ServerRoleView<InetSocketAddress, ServerInetAddressView>> servers = Lists.newLinkedList();
        for (ObjectName name: objectNames.get(schema.pathOf(Jmx.Key.REPLICA)).get()) {
            String address;
            try {
                address = (String) mbeans.getAttribute(name, QUORUM_ADDRESS_ATTRIBUTE);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            ServerInetAddressView addressView = ServerInetAddressView.fromString(address);
            EnsembleRole role = EnsembleRole.UNKNOWN;
            for (Map.Entry<EnsembleRole, ZNodeLabel.Path> entry: roles.entrySet()) {
                ZNodeLabelTrie.ValueNode<Set<ObjectName>> node = objectNames.get(entry.getValue());
                if (node != null) {
                    ObjectName nodeName = Iterables.getOnlyElement(node.get());
                    if (nodeName.getCanonicalKeyPropertyListString().startsWith(name.getCanonicalKeyPropertyListString())) {
                        role = entry.getKey();
                        break;
                    }
                }
            }
            ServerRoleView<InetSocketAddress, ServerInetAddressView> quorumView = ServerRoleView.of(addressView, role);
            servers.add(quorumView);
        }
        return EnsembleRoleView.fromRoles(servers);
    }
    
    private ServerViewJmxQuery() {}

}
