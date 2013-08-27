package edu.uw.zookeeper.clients.trace;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;

@RunWith(JUnit4.class)
public class TraceSerializationTest {

    protected final Logger logger = LogManager.getLogger(getClass());
    
    @Test
    public void testSerialize() throws IOException {
        Injector injector = Guice.createInjector(JacksonModule.create());
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        assertTrue(mapper.canDeserialize(mapper.constructType(Message.ClientRequest.class)));
        testTraceEventHeaderSerialization(TimestampEvent.currentTimeMillis(), mapper);
        testTraceEventHeaderSerialization(LatencyMeasurementEvent.create(1), mapper);
        long sessionId = 1;
        int xid = 1;
        testTraceEventHeaderSerialization(ProtocolRequestEvent.create(sessionId, ProtocolRequestMessage.of(xid, Operations.Requests.sync().build())), mapper);
        long zxid = 1;
        testTraceEventHeaderSerialization(ProtocolResponseEvent.create(sessionId, ProtocolResponseMessage.of(xid, zxid, Operations.Responses.sync().build())), mapper);
    }

    @Test
    public void testIterator() throws IOException {
        Injector injector = Guice.createInjector(JacksonModule.create());
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        StringWriter writer = new StringWriter();
        int n = 10;
        List<TraceEventHeader> events = Lists.newLinkedList();
        for (int i=0; i<n; ++i) {
            events.add(TraceEventHeader.create(TimestampEvent.create(i)));
        }
        mapper.writeValue(writer, events);
        String encoded = writer.toString();
        logger.debug(encoded);
        StringReader reader = new StringReader(encoded);
        Iterator<TraceEvent> itr = TraceEventIterator.create(
                mapper.getFactory().createParser(reader), mapper.reader());
        for (int i=0; i<10; ++i) {
            assertTrue(itr.hasNext());
            assertEquals((long) i, ((TimestampEvent) itr.next()).getTimestamp());
        }
        assertFalse(itr.hasNext());
    }
    
    public void testTraceEventHeaderSerialization(TraceEvent input, ObjectMapper mapper) throws IOException {
        testStringSerialization(TraceEventHeader.create(input), TraceEventHeader.class, mapper);
    }

    public void testStringSerialization(Object value, Class<?> cls, ObjectMapper mapper) throws IOException {
        String encoded = mapper.writeValueAsString(value);
        logger.debug(encoded);
        assertEquals(value, mapper.readValue(encoded, cls));
    }
}
