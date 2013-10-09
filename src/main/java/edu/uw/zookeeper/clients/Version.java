package edu.uw.zookeeper.clients;

import java.util.Properties;

public abstract class Version {

    public static edu.uw.zookeeper.Version getDefault() {
        return edu.uw.zookeeper.Version.fromPom(getPom());
    }
    
    public static String getGroup() {
        return "edu.uw.zookeeper.lite";
    }

    public static String getArtifact() {
        return "zklite-clients";
    }
    
    public static String getProjectName() {
        return "ZooKeeper-Lite Clients";
    }
    
    public static Properties getPom() {
        return edu.uw.zookeeper.Version.getPom(getGroup(), getArtifact());
    }
    
    private Version() {}
}
