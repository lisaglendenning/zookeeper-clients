package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import net.engio.mbassy.bus.common.PubSubSupport;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.clients.ForwardingProtocolCodec;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;

public class ProtocolTracingCodec extends ForwardingProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> {

    public static ProtocolTracingCodec defaults(
            PubSubSupport<? super TraceEvent> publisher) {
        return newInstance(publisher, ClientProtocolCodec.defaults());
    }
    
    public static ProtocolTracingCodec newInstance(
            PubSubSupport<? super TraceEvent> publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate) {
        return new ProtocolTracingCodec(
                publisher,
                delegate);
    }
    
    private final ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate;
    private final PubSubSupport<? super TraceEvent> publisher;
    private volatile long sessionId;
    
    public ProtocolTracingCodec( 
            PubSubSupport<? super TraceEvent> publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate) {
        this.delegate = delegate;
        this.publisher = publisher;
        this.sessionId = Session.uninitialized().id();
    }

    @Override
    public void encode(Message.ClientSession message, ByteBuf output) throws IOException {
        delegate.encode(message, output);
        if (sessionId != Session.uninitialized().id()) {
            Message.ClientRequest<?> request = (Message.ClientRequest<?>) message;
            switch (request.record().opcode()) {
                case PING:
                case AUTH:
                case SET_WATCHES:
                    break;
                default:
                {
                    ProtocolRequestEvent event = ProtocolRequestEvent.create(sessionId, request);
                    publisher.publish(event);
                    break;
                }
            }
        } else {
            if (! (message instanceof ConnectMessage.Request)) {
                throw new IllegalStateException();
            }
        }
    }

    @Override
    public Optional<? extends Message.ServerSession> decode(ByteBuf input)
            throws IOException {
        Optional<? extends Message.ServerSession> output = delegate.decode(input);
        if (output.isPresent()) {
            if (sessionId == Session.uninitialized().id()) {
                ConnectMessage.Response response = (ConnectMessage.Response) output.get();
                sessionId = response.getSessionId();
            } else {
                Message.ServerResponse<?> response = (Message.ServerResponse<?>) output.get();
                switch (response.record().opcode()) {
                    case PING:
                    case AUTH:
                    case SET_WATCHES:
                        break;
                    default:
                    {
                        ProtocolResponseEvent event = ProtocolResponseEvent.create(sessionId, response);
                        publisher.publish(event);
                        break;
                    }
                }
            }
        }
        return output;
    }

    @Override
    protected ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate() {
        return delegate;
    }
}
