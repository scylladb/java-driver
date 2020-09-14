Datastax Java Driver for Apache Cassandra®
==========================================

A modern, `feature-rich`_ and highly tunable Java client library for
Apache Cassandra (2.1+) and using exclusively Cassandra's binary
protocol and Cassandra Query Language v3. Use the `DataStax Enterprise
Java driver <http://docs.datastax.com/en/developer/java-driver-dse/latest>`_ for better compatibility and support for
DataStax Enterprise.

**Features:**

-  `Sync`_ and `Async`_ API
-  `Simple`_, `Prepared`_, and `Batch`_ statements
-  Asynchronous IO, parallel execution, request pipelining
-  `Connection pooling`_
-  Auto node discovery
-  Automatic reconnection
-  Configurable `load balancing`_ and `retry policies`_
-  Works with any cluster size
-  `Query builder`_
-  `Object mapper`_

The driver architecture is based on layers. At the bottom lies the
driver core. This core handles everything related to the connections to
a Cassandra cluster (for example, connection pool, discovering new
nodes, etc.) and exposes a simple, relatively low-level API on top of
which higher level layers can be built.

The driver contains the following modules:

-  driver-core: the core layer.
-  driver-mapping: the object mapper.
-  driver-extras: optional features for the Java driver.
-  driver-examples: example applications using the other modules which
   are only meant for demonstration purposes.
-  driver-tests: tests for the java-driver.

**Useful links:**

-  JIRA (bug tracking): https://datastax-oss.atlassian.net/browse/JAVA
-  MAILING LIST:
   https://groups.google.com/a/lists.datastax.com/forum/#!forum/java-driver-user
-  DATASTAX ACADEMY SLACK: #datastax-drivers on
   https://academy.datastax.com/slack
-  TWITTER: `@dsJavaDriver`_ tweets Java driver releases and important
   announcements (low frequency). `@DataStaxEng`_ has more news
   including other drivers, Cassandra, and DSE.
-  DOCS: the `manual`_ has quick start material and technical details
   about the driver and its features.
-  API: http://www.datastax.com/drivers/java/3.7
-  GITHUB REPOSITORY: https://github.com/datastax/java-driver
-  `changelog`_
-  `binary tarball`_

.. _feature-rich: manual
.. _Sync: manual
.. _Async: manual/async
.. _Simple: manual/statements/simple
.. _Prepared: manual/statements/prepared
.. _Batch: manual/statements/batch
.. _Connection pooling: manual/pooling
.. _load balancing: manual/load_balancing
.. _retry policies: manual/retries
.. _Query builder: manual/statements/built
.. _Object mapper: manual/object_mapper
.. _@dsJavaDriver: https://twitter.com/dsJavaDriver
.. _@DataStaxEng: https://twitter.com/datastaxeng
.. _manual: http://docs.datastax.com/en/developer/java-driver/3.7/manual/
.. _changelog: changelog
.. _binary tarball: http://downloads.datastax.com/java-driver/cassandra-java-driver-3.7.1.tar.gz


License
-------

Copyright 2012-2018, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

.. toctree::
   :hidden:
   :glob:

   api
   installation
   manual/*
   upgrade_guide/*
   faq/*
   changelog/*

