package com.datastax.oss.driver.api.core.cloud;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.testinfra.CassandraSkip;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.utils.PortRandomizer;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.config.scyllacloud.ScyllaCloudConnectionConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(IsolatedTests.class)
@CassandraSkip
public class ScyllaCloudMultiNodeIT {

  private static final int NUMBER_OF_NODES = 3;
  private static final int SNI_PORT = PortRandomizer.findAvailablePort();

  @ClassRule
  public static CustomCcmRule CCM_RULE =
      CustomCcmRule.builder().withNodes(NUMBER_OF_NODES).withSniProxy(SNI_PORT).build();

  @Test
  public void connect_w_simple_operations_protocol_v4() {
    String configPath = CCM_RULE.getCcmBridge().getScyllaCloudConfigPathString();
    File configFile = new File(configPath);
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, DefaultProtocolVersion.V4.toString())
            .build();
    SchemaChangeListener mockListener = Mockito.mock(SchemaChangeListenerBase.class);
    try (CqlSession session =
        CqlSession.builder()
            .withConfigLoader(loader)
            .withScyllaCloudSecureConnectBundle(configFile.toPath())
            // Currently ccm produces cloud config with eu-west-1 dc name but uses dc1
            .withLocalDatacenter("dc1")
            .withSchemaChangeListener(mockListener)
            .build()) {

      session.execute(
          String.format(
              "CREATE KEYSPACE %s "
                  + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}",
              "testks", NUMBER_OF_NODES));
      session.execute("CREATE TABLE testks.testtab (a int PRIMARY KEY, b int);");

      verify(mockListener, times(1)).onTableCreated(any(TableMetadata.class));
      verify(mockListener, times(1)).onKeyspaceCreated(any(KeyspaceMetadata.class));

      int sniPort =
          ScyllaCloudConnectionConfig.fromInputStream(Files.newInputStream(configFile.toPath()))
              .getCurrentDatacenter()
              .getServer()
              .getPort();
      assertThat(sniPort).isEqualTo(SNI_PORT);
      Map<UUID, Node> map = session.getMetadata().getNodes();
      assertThat(map.size()).isEqualTo(NUMBER_OF_NODES);
      String expectedEndpointPrefix = CCM_RULE.getCcmBridge().getIpPrefix() + "1:" + sniPort + ":";
      for (Map.Entry<UUID, Node> entry : map.entrySet()) {
        EndPoint endPoint = entry.getValue().getEndPoint();
        assertThat(endPoint.toString()).startsWith(expectedEndpointPrefix);
        assertThat(endPoint.toString()).contains(entry.getKey().toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
