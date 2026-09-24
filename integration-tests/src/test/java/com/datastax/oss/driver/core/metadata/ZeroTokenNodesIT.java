package com.datastax.oss.driver.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

@ScyllaRequirement(minOSS = "6.2.0", minEnterprise = "2025.1.0")
public class ZeroTokenNodesIT {
  // For tests to pick up @ScyllaRequirement annotation
  @ClassRule
  public static final BackendRequirementRule backendRequirementRule = new BackendRequirementRule();

  @Test
  public void should_not_ignore_zero_token_peer_when_option_is_enabled() {
    CqlSession session = null;
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder();
    try (CcmBridge ccmBridge = ccmBridgeBuilder.withNodes(3).build()) {
      ccmBridge.create();
      ccmBridge.startWithArgs("--wait-for-binary-proto");
      ccmBridge.addWithoutStart(4, "dc1");
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.startWithArgs(4, "--wait-for-binary-proto");
      DriverConfigLoader loader =
          SessionUtils.configLoaderBuilder()
              .withBoolean(DefaultDriverOption.METADATA_ALLOW_ZERO_TOKEN_PEERS, true)
              .build();
      session =
          CqlSession.builder()
              .withConfigLoader(loader)
              .addContactPoint(new InetSocketAddress(ccmBridge.getNodeIpAddress(1), 9042))
              .build();

      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<String> toStrings =
          nodes.stream().map(Node::getEndPoint).map(EndPoint::toString).collect(Collectors.toSet());

      assertThat(toStrings)
          .containsOnly(
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(1)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(2)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(3)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(4)));
    } finally {
      if (session != null) session.close();
    }
  }

  @Test
  public void should_not_discover_zero_token_DC_when_option_is_disabled() {
    CqlSession session = null;
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder();
    try (CcmBridge ccmBridge = ccmBridgeBuilder.withNodes(2, 2).build()) {
      ccmBridge.create();
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.startWithArgs("--wait-for-binary-proto");

      // Not adding .withBoolean(DefaultDriverOption.METADATA_ALLOW_ZERO_TOKEN_PEERS, false)
      // It is implicitly false
      DriverConfigLoader loader = SessionUtils.configLoaderBuilder().build();
      session =
          CqlSession.builder()
              .withLocalDatacenter("dc1")
              .withConfigLoader(loader)
              .addContactPoint(new InetSocketAddress(ccmBridge.getNodeIpAddress(1), 9042))
              .build();

      Set<String> landedOn = new HashSet<>();
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute("SELECT * FROM system.local WHERE key='local'");
        InetAddress broadcastRpcInetAddress =
            Objects.requireNonNull(rs.one()).getInetAddress("rpc_address");
        landedOn.add(Objects.requireNonNull(broadcastRpcInetAddress).toString());
      }
      assertThat(landedOn)
          .containsOnly("/" + ccmBridge.getNodeIpAddress(1), "/" + ccmBridge.getNodeIpAddress(2));
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<String> toStrings =
          nodes.stream().map(Node::getEndPoint).map(EndPoint::toString).collect(Collectors.toSet());
      assertThat(toStrings)
          .containsOnly(
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(1)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(2)));
    } finally {
      if (session != null) session.close();
    }
  }

  @Test
  public void should_discover_zero_token_DC_when_option_is_enabled() {
    CqlSession session = null;
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder();
    try (CcmBridge ccmBridge = ccmBridgeBuilder.withNodes(2, 2).build()) {
      ccmBridge.create();
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.startWithArgs("--wait-for-binary-proto");

      DriverConfigLoader loader =
          SessionUtils.configLoaderBuilder()
              .withBoolean(DefaultDriverOption.METADATA_ALLOW_ZERO_TOKEN_PEERS, true)
              .build();
      session =
          CqlSession.builder()
              .withLocalDatacenter("dc1")
              .withConfigLoader(loader)
              .addContactPoint(new InetSocketAddress(ccmBridge.getNodeIpAddress(1), 9042))
              .build();

      Set<String> landedOn = new HashSet<>();
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute("SELECT * FROM system.local WHERE key='local'");
        InetAddress broadcastRpcInetAddress =
            Objects.requireNonNull(rs.one()).getInetAddress("rpc_address");
        landedOn.add(Objects.requireNonNull(broadcastRpcInetAddress).toString());
      }
      // LBP should still target local datacenter:
      assertThat(landedOn)
          .containsOnly("/" + ccmBridge.getNodeIpAddress(1), "/" + ccmBridge.getNodeIpAddress(2));
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<String> toStrings =
          nodes.stream().map(Node::getEndPoint).map(EndPoint::toString).collect(Collectors.toSet());
      // Metadata should have all nodes:
      assertThat(toStrings)
          .containsOnly(
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(1)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(2)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(3)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(4)));
    } finally {
      if (session != null) session.close();
    }
  }

  @Test
  public void should_connect_to_zero_token_contact_point() {
    CqlSession session = null;
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder();
    try (CcmBridge ccmBridge = ccmBridgeBuilder.withNodes(2).build()) {
      ccmBridge.create();
      ccmBridge.startWithArgs("--wait-for-binary-proto");
      ccmBridge.addWithoutStart(3, "dc1");
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.startWithArgs(3, "--wait-for-binary-proto");
      // DefaultDriverOption.METADATA_ALLOW_ZERO_TOKEN_PEERS is implicitly false
      DriverConfigLoader loader = SessionUtils.configLoaderBuilder().build();
      session =
          CqlSession.builder()
              .withConfigLoader(loader)
              .addContactPoint(new InetSocketAddress(ccmBridge.getNodeIpAddress(3), 9042))
              .build();

      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<String> toStrings =
          nodes.stream().map(Node::getEndPoint).map(EndPoint::toString).collect(Collectors.toSet());
      assertThat(toStrings)
          .containsOnly(
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(1)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(2)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(3)));
    } finally {
      if (session != null) session.close();
    }
  }

  @Test
  public void should_connect_to_zero_token_DC() {
    // This test is similar but not exactly the same as should_connect_to_zero_token_contact_point.
    // In the future we may want to have different behavior for arbiter DCs and adjust this test
    // method.
    CqlSession session = null;
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder();
    try (CcmBridge ccmBridge = ccmBridgeBuilder.withNodes(2, 2).build()) {
      ccmBridge.create();
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.startWithArgs("--wait-for-binary-proto");

      DriverConfigLoader loader =
          SessionUtils.configLoaderBuilder()
              .withBoolean(DefaultDriverOption.METADATA_ALLOW_ZERO_TOKEN_PEERS, false)
              .build();
      session =
          CqlSession.builder()
              .withLocalDatacenter("dc1")
              .withConfigLoader(loader)
              .addContactPoint(new InetSocketAddress(ccmBridge.getNodeIpAddress(3), 9042))
              .build();

      Set<String> landedOn = new HashSet<>();
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute("SELECT * FROM system.local WHERE key='local'");
        InetAddress broadcastRpcInetAddress =
            Objects.requireNonNull(rs.one()).getInetAddress("rpc_address");
        landedOn.add(Objects.requireNonNull(broadcastRpcInetAddress).toString());
      }
      // LBP should still target local datacenter:
      assertThat(landedOn)
          .containsOnly("/" + ccmBridge.getNodeIpAddress(1), "/" + ccmBridge.getNodeIpAddress(2));
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<String> toStrings =
          nodes.stream().map(Node::getEndPoint).map(EndPoint::toString).collect(Collectors.toSet());
      // Metadata should have valid ordinary peers plus zero-token contact point:
      assertThat(toStrings)
          .containsOnly(
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(1)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(2)),
              String.format("/%s:9042", ccmBridge.getNodeIpAddress(3)));
    } finally {
      if (session != null) session.close();
    }
  }
}
