package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Queue;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
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

    public static ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> factory() {
        return new ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>>() {
            @Override
            public Pair<Class<Operation.Request>, AssignXidCodec> get(
                    Publisher value) {
                return Pair.create(Operation.Request.class, AssignXidCodec.newInstance(
                        AssignXidProcessor.newInstance(),
                        LatencyMeasuringCodec.newInstance(value)));
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
    }

    @Override
    public void encode(Message.ClientSession message, ByteBuf output) throws IOException {
        delegate.encode(message, output);
        if (! (message instanceof Operation.RequestId) || ! OpCodeXid.has(((Operation.RequestId) message).xid())) {
            times.offer(new RequestSentEvent(message, System.nanoTime()));
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
                RequestSentEvent pending = times.peek();
                if ((pending != null) && (pending.request instanceof ConnectMessage.Request)) {
                    times.remove(pending);
                    if (sessionId != Session.UNINITIALIZED_ID) {
                        long latency = System.nanoTime() - pending.nanos;
                        LatencyEvent event = LatencyEvent.create(latency, sessionId, pending.request, message); 
                        publisher.post(event);
                    }
                }
            } else {
                Message.ServerResponse<?> response = (Message.ServerResponse<?>) message;
                if (! OpCodeXid.has(response.xid())) {
                    assert (sessionId != Session.UNINITIALIZED_ID);
                    RequestSentEvent pending = times.peek();
                    if (pending != null) {
                        Message.ClientRequest<?> request = (Message.ClientRequest<?>) pending.request;
                        if (request.xid() == response.xid()) {
                            times.remove(pending);
                            long latency = System.nanoTime() - pending.nanos;
                            LatencyEvent event = LatencyEvent.create(latency, sessionId, pending.request, message); 
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
    
    protected static class RequestSentEvent {
        
        protected final Message.ClientSession request;
        protected final long nanos;
        
        public RequestSentEvent(Message.ClientSession request, long nanos) {
            this.request = request;
            this.nanos = nanos;
        }
    }
}