package edu.uw.zookeeper.clients.trace;

public enum TraceEventTag {
    NONE_EVENT,
    TIMESTAMP_EVENT,
    PROTOCOL_REQUEST_EVENT,
    PROTOCOL_RESPONSE_EVENT,
    LATENCY_EVENT,
    LATENCY_MEASUREMENT_EVENT;
}
