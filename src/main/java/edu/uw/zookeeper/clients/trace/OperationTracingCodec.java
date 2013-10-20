package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Queue;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

public class OperationTracingCodec implements ProtocolCodec<Message.ClientSession, Message.ServerSession> {

    public static ParameterizedFactory<PubSubSupport<Object>, Pair<Class<Message.ClientSession>, OperationTracingCodec>> factory(
            final PubSubSupport<Object> publisher) {
        return new ParameterizedFactory<PubSubSupport<Object>, Pair<Class<Message.ClientSession>, OperationTracingCodec>>() {
            @Override
            public Pair<Class<Message.ClientSession>, OperationTracingCodec> get(
                    PubSubSupport<Object> value) {
                return Pair.create(Message.ClientSession.class,
                        OperationTracingCodec.newInstance(publisher,
                                ClientProtocolCodec.newInstance(value)));
            }
        };
    }
    
    public static OperationTracingCodec newInstance(
            PubSubSupport<Object> publisher) {
        return newInstance(publisher, ClientProtocolCodec.newInstance(publisher));
    }
    
    public static OperationTracingCodec newInstance(
            PubSubSupport<Object> publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        return new OperationTracingCodec(
                publisher, 
                Queues.<RequestSentEvent>newConcurrentLinkedQueue(), 
                delegate);
    }
    
    protected final PubSubSupport<Object> publisher;
    protected final Queue<RequestSentEvent> times;
    protected final ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate;
    protected volatile long sessionId;
    
    protected OperationTracingCodec(
            PubSubSupport<Object> publisher, 
            Queue<RequestSentEvent> times,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        super();
        this.times = times;
        this.publisher = publisher;
        this.delegate = delegate;
        this.sessionId = Session.UNINITIALIZED_ID;
        
        delegate.subscribe(this);
    }

    @Override
    public void encode(Message.ClientSession message, ByteBuf output) throws IOException {
        delegate.encode(message, output);
        if (message instanceof Message.ClientRequest<?>) {
            Message.ClientRequest<?> request = (Message.ClientRequest<?>) message;
            if (! OpCodeXid.has(request.xid())) {
                times.offer(new RequestSentEvent(request, System.nanoTime()));
            }
        }
    }

    @Override
    public Optional<Message.ServerSession> decode(ByteBuf input)
            throws IOException {
        Optional<Message.ServerSession> output = delegate.decode(input);
        if (output.isPresent()) {
            Message.ServerSession message = output.get();
            if (message instanceof ConnectMessage.Response) {
                ConnectMessage.Response response = (ConnectMessage.Response) message;
                assert (sessionId == Session.UNINITIALIZED_ID);
                sessionId = response.getSessionId();
            } else {
                Message.ServerResponse<?> response = (Message.ServerResponse<?>) message;
                if (! OpCodeXid.has(response.xid())) {
                    assert (sessionId != Session.UNINITIALIZED_ID);
                    RequestSentEvent pending = times.peek();
                    if (pending != null) {
                        if (pending.request.xid() == response.xid()) {
                            times.remove(pending);
                            long latency = System.nanoTime() - pending.nanos;
                            OperationEvent event = OperationEvent.create(latency, sessionId, pending.request, response); 
                            publisher.publish(event);
                        }
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
    
    @Handler
    public void handleTransition(Automaton.Transition<?> event) {
        if (event.to() == Connection.State.CONNECTION_CLOSED) {
            try {
                delegate.unsubscribe(this);
            } catch (IllegalArgumentException e) {}
            
            RequestSentEvent pending;
            while ((pending = times.poll()) != null) {
                publisher.publish(OperationEvent.timeout(sessionId, pending.request));
            }
        }
    }
    
    protected static class RequestSentEvent {
        
        protected final Message.ClientRequest<?> request;
        protected final long nanos;
        
        public RequestSentEvent(Message.ClientRequest<?> request, long nanos) {
            this.request = request;
            this.nanos = nanos;
        }
    }
}
