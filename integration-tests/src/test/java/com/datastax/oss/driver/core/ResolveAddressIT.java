package com.datastax.oss.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class ResolveAddressIT {

  @Test
  public void keep_contact_point_unresolved_after_restart_same_cluster() {
    // These tests rely on localhost mapping to 127.0.0.1 by default
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(1).withIpPrefix("127.0.0.").build()) {
      ccmBridge.create();
      ccmBridge.start();
      try (DriverConfigLoader loader =
              new DefaultProgrammaticDriverConfigLoaderBuilder()
                  .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
                  .withStringList(
                      TypedDriverOption.CONTACT_POINTS.getRawOption(),
                      Collections.singletonList("localhost:9042"))
                  .build();
          CqlSession session = new CqlSessionBuilder().withConfigLoader(loader).build()) {

        Collection<Node> nodes = session.getMetadata().getNodes().values();
        Set<Node> filteredNodes;
        filteredNodes =
            nodes.stream()
                .filter(x -> x.toString().contains("localhost/<unresolved>:9042"))
                .collect(Collectors.toSet());
        assertThat(filteredNodes).hasSize(1);

        for (int reconnects = 0; reconnects < 3; reconnects++) {
          ccmBridge.stop();
          ccmBridge.start();

          nodes = session.getMetadata().getNodes().values();
          filteredNodes =
              nodes.stream()
                  .filter(x -> x.toString().contains("localhost/<unresolved>:9042"))
                  .collect(Collectors.toSet());
          assertThat(filteredNodes).hasSize(1);
        }
      }
    }
  }

  @Test
  public void keep_contact_point_unresolved_after_recreate_different_cluster() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("localhost:9042"))
            .build();
    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(1).withIpPrefix("127.0.0.").build()) {
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<Node> filteredNodes;
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("localhost/<unresolved>:9042"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(1).withIpPrefix("127.0.0.").build()) {
      ccmBridge.create();
      ccmBridge.start();

      Collection<Node> nodes = session.getMetadata().getNodes().values();
      Set<Node> filteredNodes;
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("localhost/<unresolved>:9042"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    session.close();
  }
}
