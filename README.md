Various ZooKeeper Java clients and utilities based on [zookeeper-lite](http://lisaglendenning.github.io/zookeeper-lite).

## Clients

- edu.uw.zookeeper.clients.jmx: utilities for querying a ZooKeeper server using JMX. 
- edu.uw.zookeeper.clients.random: utilities for generating random ZooKeeper client requests.
- edu.uw.zookeeper.clients.trace: utilities for reading and writing ZooKeeper client trace files.
- edu.uw.zookeeper.clients.trace.GetSetClient: tracing client that measures latency and throughput of a mixed getData/setData workload
- edu.uw.zookeeper.clients.trace.TraceGeneratingCacheClient: tracing client that generates random requests
- edu.uw.zookeeper.clients.trace.TraceIteratingClient: tracing client that replays requests from an existing trace file
- edu.uw.zookeeper.clients.trace.ThroughputClients: tracing client with multiple sessions
- edu.uw.zookeeper.clients.trace.csv.CsvExport: exports data from a trace file to CSV files

## Building

zookeeper-clients is a [Maven project](http://maven.apache.org/).
