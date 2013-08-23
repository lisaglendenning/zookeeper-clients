package edu.uw.zookeeper.clients.trace;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class JacksonModule extends AbstractModule {

    public static ImmutableSet<Class<? extends TraceEvent>> eventTypes = ImmutableSet.<Class<? extends TraceEvent>>of(
            TimestampEvent.class,
            LatencyMeasurementEvent.class);
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public ObjectMapper getObjectMapper() {
        ObjectMapper instance = new ObjectMapper();
        return instance;
    }
    
    @Provides @Singleton
    public JsonFactory getJsonFactory(ObjectMapper mapper) {
        return mapper.getFactory();
    }

    @Provides @Singleton
    public TraceEventSerializer getTraceEventGenerator() {
        return TraceEventSerializer.create();
    }
    
    @Provides @Singleton
    public TraceEventDeserializer getTraceEventParser() {
        return TraceEventDeserializer.forTypes(eventTypes);
    }
    
    @Provides @Singleton
    public ByteBufAllocator getByteBufAllocator() {
        return new PooledByteBufAllocator();
    }
}
