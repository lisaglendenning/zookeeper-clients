package edu.uw.zookeeper.clients.trace;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Message;

@TraceEventType(TraceEventTag.PROTOCOL_RESPONSE_EVENT)
public final class ProtocolResponseEvent implements TraceEvent {

    public static ProtocolResponseEvent create(long sessionId, Message.ServerResponse<?> response) {
        return new ProtocolResponseEvent(sessionId, response);
    }

    private final long sessionId;
    private final Message.ServerResponse<?> response;

    public ProtocolResponseEvent(long sessionId, Message.ServerResponse<?> response) {
        this.sessionId = sessionId;
        this.response = response;
    }

    @Override
    public TraceEventTag getTag() {
        return TraceEventTag.PROTOCOL_RESPONSE_EVENT;
    }

    public long getSessionId() {
        return sessionId;
    }
    
    public Message.ServerResponse<?> getResponse() {
        return response;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", sessionId)
                .add("response", response).toString();
    }
}
