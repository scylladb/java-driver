<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Control connection

The control connection is a dedicated connection used for administrative tasks:

* querying system tables to learn about the cluster’s topology and
  [schema]();
* checking [schema agreement]();
* reacting to server events, which are used to notify the driver of external topology or schema
  changes.

When the driver starts, the control connection is established to the first contacted node. If that
node goes down, a [reconnection]() is started to find another node; it is governed
by the same policy as regular connections and tries the nodes according to a query plan from the
[load balancing policy]().

The control connection is managed independently from [regular pooled connections](), and
used exclusively for administrative requests. It is included in [Session.State.getOpenConnections](https://java-driver.docs.scylladb.com/scylla-3.12.1.x/api/com/datastax/driver/core/Session.State.html#getOpenConnections-com.datastax.driver.core.Host-),
as well as the `open-connections` [metric](); for example, if you’ve configured a pool
size of 2, the control node will have 3 connections.
