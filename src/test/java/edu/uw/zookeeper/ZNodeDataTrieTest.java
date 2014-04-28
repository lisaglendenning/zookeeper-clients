package edu.uw.zookeeper;

import static org.junit.Assert.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SessionClientExecutor;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.CountingGenerator;
import edu.uw.zookeeper.clients.common.SubmitGenerator;
import edu.uw.zookeeper.clients.random.RandomRequestGenerator;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.ZNodeDataTrie;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {
    
    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=10000)
    public void testRandom() throws Exception {
        ZNodeDataTrieExecutor executor = ZNodeDataTrieExecutor.create(
                ZNodeDataTrie.newInstance(),
                ZxidIncrementer.fromZero(),
                LoggingPublisher.create(logger, EventBusPublisher.newInstance()));
        ZNodeViewCache<?, Records.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance(SessionClientExecutor.create(1, executor));
        int iterations = 100;
        Generator<Records.Request> requests = RandomRequestGenerator.defaults(cache);
        CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = CountingGenerator.create(
                iterations, iterations, SubmitGenerator.create(requests, cache), logger);
        while (operations.hasNext()) {
             Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> operation = operations.next();
             assertFalse(operation.second().get().record() instanceof Operation.Error);
        }
    }
}
