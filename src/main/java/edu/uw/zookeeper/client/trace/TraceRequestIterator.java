package edu.uw.zookeeper.client.trace;

import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class TraceRequestIterator {

    public static UnmodifiableIterator<ProtocolRequestEvent> from(
            Iterator<TraceEvent> events) {
        return Iterators.filter(events, ProtocolRequestEvent.class);
    }
    
    public static UnmodifiableIterator<ProtocolRequestEvent> forSession(
            Iterator<ProtocolRequestEvent> requests, final long sessionId) {
        return Iterators.filter(requests, new Predicate<ProtocolRequestEvent>() {
            @Override
            public boolean apply(ProtocolRequestEvent input) {
                return (input.getSessionId() == sessionId);
            }});
    }

    public static UnmodifiableIterator<ProtocolRequestEvent> forFirstSession(
            Iterator<ProtocolRequestEvent> requests) {
        return Iterators.filter(requests, new Predicate<ProtocolRequestEvent>() {
            long sessionId = Session.uninitialized().id();
            @Override
            public boolean apply(ProtocolRequestEvent input) {
                if (sessionId == Session.uninitialized().id()) {
                    sessionId = input.getSessionId();
                }
                return (input.getSessionId() == sessionId);
            }});
    }
    
    public static Iterator<Records.Request> requestsOf(
            Iterator<ProtocolRequestEvent> requests) {
        return Iterators.transform(requests, new Function<ProtocolRequestEvent, Records.Request>() {
            @Override
            public Records.Request apply(ProtocolRequestEvent input) {
                return input.getRequest().record();
            }});
    }
}
