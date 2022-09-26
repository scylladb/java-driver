# DataStax Java Driver for Apache Cassandra - Examples

This module contains examples of how to use the DataStax Java driver for
Apache Cassandra.

## Usage

Unless otherwise stated, all examples assume that you have a single-node Cassandra 3.0 cluster
listening on localhost:9042.

Before running examples, make sure you installed repo artifacts (in root driver directory):
```
mvn install -q -Dmaven.test.skip=true
```

To conveniently run the example showing OpenTelemetry integration (ZipkinConfiguration),
you can use the provided ```docker-compose.yaml``` file (which runs both Scylla cluster and Zipkin instance):
```
docker-compose up
./runOpenTelemetryExample.sh
```
