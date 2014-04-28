package edu.uw.zookeeper;

import static org.junit.Assert.assertFalse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.CountingGenerator;
import edu.uw.zookeeper.clients.common.SubmitGenerator;
import edu.uw.zookeeper.clients.random.RandomRequestGenerator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class RandomSingleClientTest {

    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=30000)
    public void testRandom() throws Exception {
        SimpleServerAndClient client = SimpleServerAndClient.defaults().setDefaults();
        ServiceMonitor monitor = client.getRuntimeModule().getServiceMonitor();
        for (Service service: client.build()) {
            monitor.add(service);
        }
        monitor.startAsync().awaitRunning();
        
        ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance( 
                        client.getClientBuilder().getConnectionClientExecutor());
        int iterations = 100;
        Generator<Records.Request> requests = RandomRequestGenerator.defaults(cache);
        CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                CountingGenerator.create(iterations, iterations, 
                        SubmitGenerator.create(requests, cache), logger);
        while (operations.hasNext()) {
             Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> operation = operations.next();
             assertFalse(operation.second().get().record() instanceof Operation.Error);
        }
        monitor.stopAsync().awaitTerminated();
    }
}
