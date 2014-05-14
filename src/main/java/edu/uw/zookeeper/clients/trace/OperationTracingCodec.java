package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Queue;

import net.engio.mbassy.bus.common.PubSubSupport;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.clients.ForwardingProtocolCodec;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

public class OperationTracingCodec extends ForwardingProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> implements Automatons.AutomatonListener<ProtocolState> {

    public static OperationTracingCodec defaults(
            PubSubSupport<? super TraceEvent> publisher) {
        return newInstance(publisher, ClientProtocolCodec.defaults());
    }
    
    public static OperationTracingCodec newInstance(
            PubSubSupport<? super TraceEvent> publisher,
            ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate) {
        return new OperationTracingCodec(
                publisher, 
                Queues.<RequestSentEvent>newConcurrentLinkedQueue(), 
                delegate);
    }
    
    protected final PubSubSupport<? super TraceEvent> publisher;
    protected final Queue<RequestSentEvent> times;
    protected final ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate;
    protected volatile long sessionId;
    
    protected OperationTracingCodec(
            PubSubSupport<? super TraceEvent> publisher, 
            Queue<RequestSentEvent> times,
            ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate) {
        super();
        this.times = times;
        this.publisher = publisher;
        this.delegate = delegate;
        this.sessionId = Session.uninitialized().id();
        
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
    public Optional<? extends Message.ServerSession> decode(ByteBuf input)
            throws IOException {
        Optional<? extends Message.ServerSession> output = delegate.decode(input);
        if (output.isPresent()) {
            Message.ServerSession message = output.get();
            if (message instanceof ConnectMessage.Response) {
                ConnectMessage.Response response = (ConnectMessage.Response) message;
                sessionId = response.getSessionId();
            } else {
                Message.ServerResponse<?> response = (Message.ServerResponse<?>) message;
                if (! OpCodeXid.has(response.xid())) {
                    RequestSentEvent pending = times.peek();
                    if (pending != null) {
                        if (pending.request.xid() == response.xid()) {
                            times.remove(pending);
                            long latency = System.nanoTime() - pending.nanos;
                            OperationEvent event = OperationEvent.create(latency, sessionId, pending.request, response); 
                            publisher.publish(event);
                        }
                    }
                    if (response.record().opcode() == OpCode.CLOSE_SESSION) {
                        delegate.unsubscribe(this);
                        assert(times.isEmpty());
                    }
                }
            }
        }
        return output;
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        switch (transition.to()) {
        case ERROR:
        {
            delegate.unsubscribe(this);
            RequestSentEvent pending;
            while ((pending = times.poll()) != null) {
                publisher.publish(OperationEvent.timeout(sessionId, pending.request));
            }
            break;
        }
        default:
            break;
        }
    }

    @Override
    protected ProtocolCodec<Message.ClientSession, Message.ServerSession, Message.ClientSession, Message.ServerSession> delegate() {
        return delegate;
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
