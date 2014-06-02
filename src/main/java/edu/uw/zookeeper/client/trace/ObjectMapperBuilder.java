package edu.uw.zookeeper.client.trace;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.clients.Version;
import edu.uw.zookeeper.jackson.databind.ProtocolRequestDeserializer;
import edu.uw.zookeeper.jackson.databind.ProtocolRequestSerializer;
import edu.uw.zookeeper.protocol.Message;

public class ObjectMapperBuilder extends edu.uw.zookeeper.jackson.databind.ObjectMapperBuilder {

    public static ObjectMapperBuilder defaults() {
        return new ObjectMapperBuilder();
    }
    
    public ObjectMapperBuilder() {}

    @Override
    protected ObjectMapper getDefaultObjectMapper() {
        ObjectMapper mapper = super.getDefaultObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }
    
    @Override
    protected List<Module> getDefaultModules() {
        return ImmutableList.<Module>of(new JacksonModuleBuilder().build());
    }
    
    public static class JacksonModuleBuilder extends edu.uw.zookeeper.jackson.databind.JacksonModuleBuilder {

        public JacksonModuleBuilder() {}
        
        @Override
        protected String getDefaultProjectName() {
            return Version.getProjectName();
        }

        @Override
        protected com.fasterxml.jackson.core.Version getDefaultVersion() {
            edu.uw.zookeeper.Version version = Version.getDefault();
            return new com.fasterxml.jackson.core.Version(
                    version.getMajor(),
                    version.getMinor(),
                    version.getPatch(),
                    version.getLabel(),
                    Version.getGroup(),
                    Version.getArtifact());
        }
        
        @Override
        protected List<JsonSerializer<?>> getDefaultSerializers() {
            return ImmutableList.<JsonSerializer<?>>of(
                    ProtocolRequestSerializer.create(),
                    ProtocolResponseHeaderSerializer.create(),
                    TraceEventHeader.serializer());
        }

        @Override
        protected Map<Class<?>, JsonDeserializer<?>> getDefaultDeserializers() {
            return ImmutableMap.<Class<?>, JsonDeserializer<?>>of(
                    Message.ClientRequest.class, ProtocolRequestDeserializer.create(),
                    Message.ServerResponse.class, ProtocolResponseHeaderDeserializer.create(),
                    TraceEventHeader.class, TraceEventHeader.deserializer());
        }
    }
}
