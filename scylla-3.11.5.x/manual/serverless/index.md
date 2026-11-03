# Connecting to ScyllaDB Cloud Serverless

With ScyllaDB Cloud, you can deploy [serverless databases](https://cloud.docs.scylladb.com/stable/serverless/index.html).
The Java driver allows you to connect to a serverless database by utilizing the connection bundle you can download via the **Connect>Java** tab in the Cloud application.
The connection bundle is a YAML file with connection and credential information for your cluster.

Connecting to a ScyllaDB Cloud serverless database is very similar to a standard connection to a ScyllaDB database.

Here’s a short program that connects to a ScyllaDB Cloud serverless database and executes a query:

```java
Cluster cluster = null;
try {
    File bundleFile = new File("/file/downloaded/from/cloud/connect-bundle.yaml");
        
    cluster = Cluster.builder()                                                  // (1)
            .withScyllaCloudConnectionConfig(bundleFile)                         // (2)
            .build();
    Session session = cluster.connect();                                         // (3)

    ResultSet rs = session.execute("select release_version from system.local");  // (4)
    Row row = rs.one();
    System.out.println(row.getString("release_version"));                        // (5)
} finally {
    if (cluster != null) cluster.close();                                        // (6)
}
```

1. The [Cluster](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Cluster.html) object is the main entry point of the driver. It holds the known state of the actual ScyllaDB cluster
   (notably the [Metadata]()). This class is thread-safe, you should create a single instance (per target
   ScyllaDB cluster), and share it throughout your application;
2. [withScyllaCloudConnectionConfig](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Cluster.Builder.html#withScyllaCloudConnectionConfig-java.io.File-) is a method that configures the cluster endpoints and credentials
   to your ScyllaDB Cloud serverless cluster based on the YAML connection bundle you downloaded from ScyllaDB Cloud;
3. The [Session](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Session.html) is what you use to execute queries. Likewise, it is thread-safe and should be reused;
4. We use `execute` to send a query to Cassandra. This returns a [ResultSet](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/ResultSet.html), which is essentially a collection of [Row](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Row.html)
   objects. On the next line, we extract the first row (which is the only one in this case);
5. We extract the value of the first (and only) column from the row;
6. Finally, we close the cluster after we’re done with it. This will also close any session that was created from this
   cluster. This step is important because it frees underlying resources (TCP connections, thread pools…). In a real
   application, you would typically do this at shutdown (for example, when undeploying your webapp).
