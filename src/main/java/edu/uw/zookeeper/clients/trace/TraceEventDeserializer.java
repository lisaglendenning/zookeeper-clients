package edu.uw.zookeeper.clients.trace;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Pair;

public class TraceEventDeserializer {
    
    public static TraceEventDeserializer forTypes(Iterable<? extends Class<?>> types) {
        ImmutableMap.Builder<TraceEventTag, Pair<? extends Class<?>, Method>> parsers = ImmutableMap.builder();
        for (Class<?> type: types) {
            for (Method method: type.getMethods()) {
                JacksonDeserializer annotation = method.getAnnotation(JacksonDeserializer.class);
                if (annotation != null) {
                    Class<?> parsedType = (Modifier.isStatic(method.getModifiers())) ?
                        method.getReturnType() : type;
                    TraceEventTag tag = parsedType.getAnnotation(TraceEventType.class).value();
                    parsers.put(tag, Pair.create(parsedType, method));
                    break;
                }
            }
        }
        return new TraceEventDeserializer(parsers.build());
    }
    
    protected static final TraceEventTag[] EVENT_TAGS = TraceEventTag.values();
    
    protected final Map<TraceEventTag, Pair<? extends Class<?>, Method>> parsers;

    protected TraceEventDeserializer(Map<TraceEventTag, Pair<? extends Class<?>, Method>> parsers) {
        this.parsers = parsers;
    }
    
    @JacksonDeserializer
    public TraceEvent deserialize(JsonParser parser) throws JsonParseException, IOException {
        TraceEventTag tag = EVENT_TAGS[parser.getIntValue()];
        Pair<? extends Class<?>, Method> p = parsers.get(tag);
        TraceEvent instance;
        try {
            if (Modifier.isStatic(p.second().getModifiers())) {
                    instance = (TraceEvent) p.second().invoke(null, parser);
            } else {
                instance = (TraceEvent) p.first().newInstance();
                p.second().invoke(instance, parser);
            }
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (InvocationTargetException e) {
            throw new AssertionError(e);
        } catch (InstantiationException e) {
            throw new AssertionError(e);
        }
        return instance;
    }
}
