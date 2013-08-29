package edu.uw.zookeeper.clients.trace;

import java.util.Date;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

public final class TraceHeader {

    public static TraceHeader create(
            String description,
            TraceEventTag...types) {
        return create(description, ImmutableSet.copyOf(types));
    }
    
    public static TraceHeader create(
            String description,
            Set<TraceEventTag> types) {
        return new TraceHeader(new Date(), description, types);
    }
    
    protected final Date date;
    protected final String description;
    protected final ImmutableSet<TraceEventTag> types;
    
    @JsonCreator
    public TraceHeader(
            @JsonProperty("date") Date date, 
            @JsonProperty("description") String description,
            @JsonProperty("types") Set<TraceEventTag> types) {
        super();
        this.date = date;
        this.description = description;
        this.types = ImmutableSet.copyOf(types);
    }
    
    public Date getDate() {
        return date;
    }
    
    public String getDescription() {
        return description;
    }
    
    public Set<TraceEventTag> getTypes() {
        return types;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("date", date).add("description", description).add("types", types).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof TraceHeader)) {
            return false;
        }
        TraceHeader other = (TraceHeader) obj;
        return Objects.equal(date, other.date)
                && Objects.equal(description, other.description)
                && Objects.equal(types, other.types);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(date, description, types);
    }
}
