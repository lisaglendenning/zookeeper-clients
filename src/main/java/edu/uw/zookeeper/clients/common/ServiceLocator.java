package edu.uw.zookeeper.clients.common;

public interface ServiceLocator {
    <T> T getInstance(Class<T> type);
}
