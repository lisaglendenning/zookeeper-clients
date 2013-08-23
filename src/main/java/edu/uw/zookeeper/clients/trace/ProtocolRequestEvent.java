package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Message;

@TraceEventType(TraceEventTag.PROTOCOL_REQUEST_EVENT)
public final class ProtocolRequestEvent implements TraceEvent {

    public static ProtocolRequestEvent create(long sessionId, Message.ClientRequest<?> request) {
        return new ProtocolRequestEvent(sessionId, request);
    }

    private final long sessionId;
    private final Message.ClientRequest<?> request;

    public ProtocolRequestEvent(long sessionId, Message.ClientRequest<?> request) {
        this.sessionId = sessionId;
        this.request = request;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.PROTOCOL_REQUEST_EVENT;
    }

    public long getSessionId() {
        return sessionId;
    }
    
    public Message.ClientRequest<?> getRequest() {
        return request;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", sessionId)
                .add("request", request).toString();
    }
}
