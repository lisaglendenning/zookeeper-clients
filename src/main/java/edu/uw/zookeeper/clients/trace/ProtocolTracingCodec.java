package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import net.engio.mbassy.PubSubSupport;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;

public class ProtocolTracingCodec implements ProtocolCodec<Message.ClientSession, Message.ServerSession> {

    public static ParameterizedFactory<PubSubSupport<Object>, Pair<Class<Message.ClientSession>, ProtocolTracingCodec>> factory(
            final PubSubSupport<Object> publisher) {
        return new ParameterizedFactory<PubSubSupport<Object>, Pair<Class<Message.ClientSession>, ProtocolTracingCodec>>() {
            @Override
            public Pair<Class<Message.ClientSession>, ProtocolTracingCodec> get(
                    PubSubSupport<Object> value) {
                return Pair.create(Message.ClientSession.class,
                        ProtocolTracingCodec.newInstance(publisher,
                                ClientProtocolCodec.newInstance(value)));
            }
        };
    }
    
    public static ProtocolTracingCodec newInstance(
            PubSubSupport<Object> publisher) {
        return newInstance(publisher, ClientProtocolCodec.newInstance(publisher));
    }
    
    public static ProtocolTracingCodec newInstance(
            PubSubSupport<Object> publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        return new ProtocolTracingCodec(
                publisher,
                delegate);
    }
    
    private final ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate;
    private final PubSubSupport<Object> publisher;
    private volatile long sessionId;
    
    public ProtocolTracingCodec( 
            PubSubSupport<Object> publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        this.delegate = delegate;
        this.publisher = publisher;
        this.sessionId = Session.UNINITIALIZED_ID;
    }

    @Override
    public void encode(Message.ClientSession message, ByteBuf output) throws IOException {
        delegate.encode(message, output);
        if (sessionId != Session.UNINITIALIZED_ID) {
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
    public Optional<Message.ServerSession> decode(ByteBuf input)
            throws IOException {
        Optional<Message.ServerSession> output = delegate.decode(input);
        if (output.isPresent()) {
            if (sessionId == Session.UNINITIALIZED_ID) {
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
    public ProtocolState state() {
        return delegate.state();
    }

    @Override
    public void publish(Object event) {
        delegate.publish(event);
    }

    @Override
    public void subscribe(Object handler) {
        delegate.subscribe(handler);
        publisher.subscribe(handler);
    }

    @Override
    public boolean unsubscribe(Object handler) {
        boolean unsubscribed = delegate.unsubscribe(handler);
        try {
            unsubscribed = publisher.unsubscribe(handler) || unsubscribed;
        } catch (IllegalArgumentException e) {}
        return unsubscribed;
    }
}
