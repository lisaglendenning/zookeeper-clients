package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Factories;

public final class TraceEventHeader extends Factories.Holder<TraceEvent> {

    public static TraceEventHeader create(TraceEvent value) {
        return new TraceEventHeader(value);
    }

    public static Serializer serializer() {
        return Serializer.create();
    }

    public static Deserializer deserializer() {
        return Deserializer.create();
    }

    public static Map<TraceEventTag, Class<? extends TraceEvent>> types() {
        ImmutableMap.Builder<TraceEventTag, Class<? extends TraceEvent>> builder = ImmutableMap.builder();
        ImmutableList<Class<? extends TraceEvent>> types = ImmutableList.of(
                TimestampEvent.class, 
                ProtocolRequestEvent.class, 
                ProtocolResponseEvent.class, 
                LatencyEvent.class, 
                LatencyMeasurementEvent.class);
        for (Class<? extends TraceEvent> type: types) {
            builder.put(type.getAnnotation(TraceEventType.class).value(), type);
        }
        return builder.build();
    }
    
    public TraceEventHeader(TraceEvent value) {
        super(value);
    }

    public static class Serializer extends StdSerializer<TraceEventHeader> {
    
        public static Serializer create() {
            return new Serializer();
        }
        
        public Serializer() {
            super(TraceEventHeader.class);
        }
    
        @Override
        public void serialize(TraceEventHeader value, JsonGenerator json,
                SerializerProvider provider) throws IOException,
                JsonGenerationException {
            TraceEvent event = value.get();
            json.writeStartArray();
            TraceEventTag tag = event.getTag();
            json.writeNumber(tag.ordinal());
            provider.findValueSerializer(event.getClass(), null).serialize(event, json, provider);
            json.writeEndArray();
        }
    
        @Override
        public JsonNode getSchema(SerializerProvider provider, Type typeHint)
            throws JsonMappingException {
            return createSchemaNode("array");
        }
    }
    
    public static class Deserializer extends StdDeserializer<TraceEventHeader> {

        public static Deserializer create() {
            return create(types());
        }
        
        public static Deserializer create(Map<TraceEventTag, Class<? extends TraceEvent>> types) {
            return new Deserializer(types);
        }
        
        protected static final TraceEventTag[] EVENT_TAGS = TraceEventTag.values();
        
        private static final long serialVersionUID = -1819939360080426783L;

        protected final Map<TraceEventTag, Class<? extends TraceEvent>> types;

        public Deserializer(Map<TraceEventTag, Class<? extends TraceEvent>> types) {
            super(TraceEventHeader.class);
            this.types = types;
        }
        
        @Override
        public TraceEventHeader deserialize(JsonParser json,
                DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            if (! json.isExpectedStartArrayToken()) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.START_ARRAY.toString());
            }
            json.nextToken();
            TraceEventTag tag = EVENT_TAGS[json.getIntValue()];
            json.nextToken();
            Class<? extends TraceEvent> type = types.get(tag);
            TraceEvent value = (TraceEvent) ctxt.findContextualValueDeserializer(ctxt.constructType(type), null).deserialize(json, ctxt);
            if (json.getCurrentToken() != JsonToken.END_ARRAY) {
                throw ctxt.wrongTokenException(json, json.getCurrentToken(), JsonToken.END_ARRAY.toString());
            }
            json.nextToken();
            return new TraceEventHeader(value);
        }
        
        @Override
        public boolean isCachable() { 
            return true; 
        }
    }
}
