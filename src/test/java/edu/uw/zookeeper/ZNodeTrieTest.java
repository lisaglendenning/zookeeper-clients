package edu.uw.zookeeper;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SessionClientExecutor;
import edu.uw.zookeeper.data.ZNodeCacheTrie;
import edu.uw.zookeeper.clients.common.CallUntilPresent;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.clients.random.BasicRequestGenerator;
import edu.uw.zookeeper.common.ListAccumulator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class ZNodeTrieTest {
    
    protected final Logger logger = LogManager.getLogger();
    
    @Test //(timeout=10000)
    public void testRandom() throws Exception {
        ZNodeTrieExecutor executor = ZNodeTrieExecutor.defaults();
        ZNodeCacheTrie<ZNodeCacheTrie.SimpleCachedNode, Records.Request, Message.ServerResponse<?>> cache = 
                ZNodeCacheTrie.newInstance(SessionClientExecutor.create(1, executor));
        int iterations = 100;
        Generator<Records.Request> requests = BasicRequestGenerator.create(cache);
        ListAccumulator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> accumulator = ListAccumulator.create(
                SubmitCallable.create(requests, cache),
                Lists.<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>>newArrayListWithCapacity(iterations)); 
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> results = 
                CallUntilPresent.create(IterationCallable.create(iterations, iterations, accumulator)).call();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> result: results) {
            Message.ServerResponse<?> response = result.second().get();
            assertFalse(String.format("%s => %s", result.first(), response), response.record() instanceof Operation.Error);
        }
    }
}