package edu.uw.zookeeper.client.trace;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface TraceEventType {
    TraceEventTag value();
}
