package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.base.Optional;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;

public class ProtocolTracingCodec implements ProtocolCodec<Message.ClientSession, Message.ServerSession> {

    public static ParameterizedFactory<Publisher, Pair<Class<Message.ClientSession>, ProtocolTracingCodec>> factory(
            final Publisher publisher) {
        return new ParameterizedFactory<Publisher, Pair<Class<Message.ClientSession>, ProtocolTracingCodec>>() {
            @Override
            public Pair<Class<Message.ClientSession>, ProtocolTracingCodec> get(
                    Publisher value) {
                return Pair.create(Message.ClientSession.class,
                        ProtocolTracingCodec.newInstance(publisher,
                                ClientProtocolCodec.newInstance(value)));
            }
        };
    }
    
    public static ProtocolTracingCodec newInstance(
            Publisher publisher) {
        return newInstance(publisher, ClientProtocolCodec.newInstance(publisher));
    }
    
    public static ProtocolTracingCodec newInstance(
            Publisher publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        return new ProtocolTracingCodec(
                publisher,
                delegate);
    }
    
    private final ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate;
    private final Publisher publisher;
    private volatile long sessionId;
    
    public ProtocolTracingCodec( 
            Publisher publisher,
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
                    publisher.post(event);
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
                        publisher.post(event);
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
    public void register(Object handler) {
        delegate.register(handler);
        publisher.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate.unregister(handler);
        try {
            publisher.unregister(handler);
        } catch (IllegalArgumentException e) {}
    }
}
