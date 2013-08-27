package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Queue;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

public class LatencyMeasuringCodec implements ProtocolCodec<Message.ClientSession, Message.ServerSession> {

    public static ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> factory(
            final Publisher publisher) {
        return new ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>>() {
            @Override
            public Pair<Class<Operation.Request>, AssignXidCodec> get(
                    Publisher value) {
                return Pair.create(Operation.Request.class, AssignXidCodec.newInstance(
                        AssignXidProcessor.newInstance(),
                        LatencyMeasuringCodec.newInstance(publisher,
                                ClientProtocolCodec.newInstance(publisher))));
            }
        };
    }
    
    public static LatencyMeasuringCodec newInstance(
            Publisher publisher) {
        return newInstance(publisher, ClientProtocolCodec.newInstance(publisher));
    }
    
    public static LatencyMeasuringCodec newInstance(
            Publisher publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        return new LatencyMeasuringCodec(
                publisher, 
                Queues.<RequestSentEvent>newConcurrentLinkedQueue(), 
                delegate);
    }
    
    protected final Publisher publisher;
    protected final Queue<RequestSentEvent> times;
    protected final ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate;
    protected volatile long sessionId;
    
    protected LatencyMeasuringCodec(
            Publisher publisher, 
            Queue<RequestSentEvent> times,
            ProtocolCodec<Message.ClientSession, Message.ServerSession> delegate) {
        super();
        this.times = times;
        this.publisher = publisher;
        this.delegate = delegate;
        this.sessionId = Session.UNINITIALIZED_ID;
        
        delegate.register(this);
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
                            LatencyEvent event = LatencyEvent.create(latency, sessionId, pending.request, response); 
                            publisher.post(event);
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
    
    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (event.to() == Connection.State.CONNECTION_CLOSED) {
            try {
                delegate.unregister(this);
            } catch (IllegalArgumentException e) {}
            
            RequestSentEvent pending;
            while ((pending = times.poll()) != null) {
                publisher.post(LatencyEvent.timeout(sessionId, pending.request));
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
