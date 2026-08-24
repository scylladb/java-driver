/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BindNodeException;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import java.util.concurrent.ExecutionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for JAVA-2654. */
public class PeersV2NodeRefreshIT {

  // Simulacron's "multiple nodes per IP" mode (needed here to emulate several nodes sharing a
  // single address, as system.peers_v2 distinguishes nodes by (peer, peer_port)) always binds
  // nodes to consecutive ports on a loopback address starting at port 49152 -- the first port of
  // the OS ephemeral/dynamic port range on both Linux (/proc/sys/net/ipv4/ip_local_port_range,
  // typically 32768-60999) and macOS (49152-65535). That range is also what the kernel hands out
  // as the *source* port for any outbound client socket opened by any process on the machine,
  // including other integration tests' driver connections running concurrently in this same
  // build. When the kernel happens to allocate 49152 as an ephemeral source port at the exact
  // moment this test tries to bind it as a *listening* socket, Simulacron fails with
  // BindNodeException ("Failed to bind ... to /127.0.0.1:49152"). This is a real,
  // previously-reported flake (see https://github.com/scylladb/java-driver/issues/951, which was
  // closed after an unrelated fix for a different flaky test happened to land around the same
  // time as a CI re-run that passed -- the actual Simulacron port race was never fixed).
  //
  // Ideally this test would sidestep the collision entirely by binding to a fixed, non-ephemeral
  // starting port via Server.Builder#withAddressResolver(...). However, the vendored
  // com.scylladb.oss.simulacron:simulacron-native-server:0.14.0.0 unconditionally re-creates a
  // brand-new `new NodePerPortResolver()` (hardcoded to port 49152) inside build() whenever
  // withMultipleNodesPerIp(true) is set, silently discarding any resolver configured via
  // withAddressResolver(...) -- confirmed by decompiling the actual jar (this is a deviation
  // from the upstream datastax/simulacron behavior, where withAddressResolver called after
  // withMultipleNodesPerIp is honored). Since the starting port can't be changed through the
  // public API, the only effective mitigation available here is to retry the whole
  // register-and-bind attempt with a fresh Server/resolver and a short backoff, which gives the
  // OS a chance to move its ephemeral allocator away from port 49152 before the next attempt.
  private static final int MAX_BIND_ATTEMPTS = 5;

  private static Server peersV2Server;
  private static BoundCluster cluster;

  @BeforeClass
  public static void setup() throws InterruptedException {
    RuntimeException lastFailure = null;
    for (int attempt = 1; attempt <= MAX_BIND_ATTEMPTS; attempt++) {
      Server server = Server.builder().withMultipleNodesPerIp(true).build();
      try {
        cluster = server.register(ClusterSpec.builder().withNodes(2));
        peersV2Server = server;
        return;
      } catch (RuntimeException e) {
        // Always tear down the server we just created, whether or not we're going to retry, to
        // avoid leaking its event loop threads.
        server.close();
        boolean isBindRace = e.getCause() instanceof BindNodeException;
        if (!isBindRace || attempt == MAX_BIND_ATTEMPTS) {
          throw e;
        }
        lastFailure = e;
        Thread.sleep(200L * attempt);
      }
    }
    // Unreachable, but keeps the compiler happy about a definite return/throw.
    throw lastFailure;
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stop();
    }
    if (peersV2Server != null) {
      peersV2Server.close();
    }
  }

  @Test
  public void should_successfully_send_peers_v2_node_refresh_query()
      throws InterruptedException, ExecutionException {
    try (CqlSession session =
        CqlSession.builder().addContactPoint(cluster.node(1).inetSocketAddress()).build()) {
      Node node = findNonControlNode(session);
      ((InternalDriverContext) session.getContext())
          .getMetadataManager()
          .refreshNode(node)
          .toCompletableFuture()
          .get();
      assertThat(hasNodeRefreshQuery())
          .describedAs("Expecting peers_v2 node refresh query to be present but it wasn't")
          .isTrue();
    }
  }

  private Node findNonControlNode(CqlSession session) {
    EndPoint controlNode =
        ((InternalDriverContext) session.getContext())
            .getControlConnection()
            .channel()
            .getEndPoint();
    return session.getMetadata().getNodes().values().stream()
        .filter(node -> !node.getEndPoint().equals(controlNode))
        .findAny()
        .orElseThrow(() -> new IllegalStateException("Expecting at least one non-control node"));
  }

  private boolean hasNodeRefreshQuery() {
    for (QueryLog log : cluster.getLogs().getQueryLogs()) {
      if (log.getFrame().message instanceof Query) {
        // Match both the legacy "SELECT *" form and the optimized projected-column form;
        // only the WHERE clause suffix is stable regardless of which columns are selected.
        if (((Query) log.getFrame().message)
            .query.contains("FROM system.peers_v2 WHERE peer = :address and peer_port = :port")) {
          return true;
        }
      }
    }
    return false;
  }
}
