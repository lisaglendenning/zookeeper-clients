package edu.uw.zookeeper;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.bus.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.data.ZNodeDataTrie.Operators;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ByOpcodeTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.RequestErrorProcessor;
import edu.uw.zookeeper.protocol.server.ToTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;

public class ZNodeDataTrieExecutor implements PubSubSupport<Object>, ClientExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>>,
        Processors.UncheckedProcessor<SessionOperation.Request<?>, Message.ServerResponse<?>> {

    @SuppressWarnings("rawtypes")
    public static ZNodeDataTrieExecutor create() {
        return create(
                ZNodeDataTrie.newInstance(),
                ZxidIncrementer.fromZero(),
                new SyncMessageBus<Object>(new SyncBusConfiguration()));
    }

    public static ZNodeDataTrieExecutor create(
            ZNodeDataTrie trie,
            ZxidGenerator zxids,
            PubSubSupport<Object> publisher) {
        return new ZNodeDataTrieExecutor(trie, zxids, publisher);
    }
    
    protected final PubSubSupport<Object> publisher;
    protected final ZNodeDataTrie trie;
    protected final RequestErrorProcessor<TxnOperation.Request<?>> operator;
    protected final ToTxnRequestProcessor txnProcessor;
    
    public ZNodeDataTrieExecutor(
            ZNodeDataTrie trie,
            ZxidGenerator zxids,
            PubSubSupport<Object> publisher) {
        this.trie = trie;
        this.publisher = publisher;
        this.txnProcessor = ToTxnRequestProcessor.create(
                AssignZxidProcessor.newInstance(zxids));
        this.operator = RequestErrorProcessor.create(
                ByOpcodeTxnRequestProcessor.create(
                        ImmutableMap.copyOf(Operators.of(trie))));
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request) {
        return submit(request, SettableFuturePromise.<Message.ServerResponse<?>>create());
    }

    @Override
    public synchronized ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request,
            Promise<Message.ServerResponse<?>> promise) {
        Message.ServerResponse<?> result = apply(request);
        promise.set(result);
        return promise;
    }
    
    @Override
    public synchronized Message.ServerResponse<?> apply(SessionOperation.Request<?> input) {
        TxnOperation.Request<?> request = txnProcessor.apply(input);
        Message.ServerResponse<Records.Response> response = ProtocolResponseMessage.of(request.xid(), request.zxid(), operator.apply(request));
        publish(response);
        return response;
    }

    @Override
    public void subscribe(Object handler) {
        publisher.subscribe(handler);
    }

    @Override
    public boolean unsubscribe(Object handler) {
        return publisher.unsubscribe(handler);
    }
    
    @Override
    public void publish(Object event) {
        publisher.publish(event);
    }
}