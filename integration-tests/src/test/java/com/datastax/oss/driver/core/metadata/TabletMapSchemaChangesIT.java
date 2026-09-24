package com.datastax.oss.driver.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.KeyspaceTableNamePair;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.testinfra.ScyllaOnly;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.metadata.schema.TabletMapSchemaChangeListener;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ScyllaRequirement(
    minOSS = "6.0.0",
    minEnterprise = "2024.2",
    description = "Needs to support tablets")
@ScyllaOnly(description = "Tablets are ScyllaDB-only extension")
// Ensures that TabletMap used by MetadataManager behaves as desired on certain events
public class TabletMapSchemaChangesIT {

  // Same listener as the one registered on initialization by
  // DefaultDriverContext#buildSchemaChangeListener
  // for TabletMap updates. Note that this mock only verifies that it reacts to ".onXhappening()"
  // calls and the
  // actual working listener updates the TabletMap.
  private static final TabletMapSchemaChangeListener listener =
      Mockito.mock(TabletMapSchemaChangeListener.class);
  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder()
          .withNodes(2)
          .withCassandraConfiguration(
              "experimental_features", "['consistent-topology-changes','tablets']")
          .build();
  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                  .withClass(
                      DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                      BasicLoadBalancingPolicy.class)
                  .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, true)
                  .withDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW, Duration.ofSeconds(0))
                  .build())
          .withSchemaChangeListener(listener)
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final int INITIAL_TABLETS = 32;
  private static final int REPLICATION_FACTOR = 1;
  private static final String KEYSPACE_NAME = "TabletMapSchemaChangesIT";
  private static final String TABLE_NAME = "testTable";
  private static final KeyspaceTableNamePair TABLET_MAP_KEY =
      new KeyspaceTableNamePair(
          CqlIdentifier.fromCql(KEYSPACE_NAME), CqlIdentifier.fromCql(TABLE_NAME));
  private static final String CREATE_KEYSPACE_QUERY =
      "CREATE KEYSPACE IF NOT EXISTS "
          + KEYSPACE_NAME
          + " WITH replication = {'class': "
          + "'NetworkTopologyStrategy', "
          + "'replication_factor': '"
          + REPLICATION_FACTOR
          + "'}  AND durable_writes = true AND tablets = "
          + "{'initial': "
          + INITIAL_TABLETS
          + "};";
  private static final String CREATE_TABLE_QUERY =
      "CREATE TABLE IF NOT EXISTS "
          + KEYSPACE_NAME
          + "."
          + TABLE_NAME
          + " (pk int, ck int, PRIMARY KEY(pk, ck));";
  private static final String DROP_KEYSPACE = "DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME;

  private static final String INSERT_QUERY_TEMPLATE =
      "INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (pk, ck) VALUES (%s, %s)";
  private static final String SELECT_QUERY_TEMPLATE =
      "SELECT pk, ck FROM " + KEYSPACE_NAME + "." + TABLE_NAME + " WHERE  pk = ?";

  private static final long NOTIF_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);

  @Before
  public void setup() {
    SESSION_RULE.session().execute(DROP_KEYSPACE);
    SESSION_RULE.session().execute(CREATE_KEYSPACE_QUERY);
    SESSION_RULE.session().execute(CREATE_TABLE_QUERY);
    SESSION_RULE.session().execute(String.format(INSERT_QUERY_TEMPLATE, "1", "2"));
    SESSION_RULE.session().execute(String.format(INSERT_QUERY_TEMPLATE, "3", "4"));
    PreparedStatement ps = SESSION_RULE.session().prepare(SELECT_QUERY_TEMPLATE);
    BoundStatement bs = ps.bind(1);
    // This ensures we hit the node that is not tablet replica
    for (Node node : SESSION_RULE.session().getMetadata().getNodes().values()) {
      SESSION_RULE.session().execute(bs.setNode(node));
    }
    // Make sure the tablet information is present
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                SESSION_RULE.session().getMetadata().getTabletMap().isPresent()
                    && SESSION_RULE
                        .session()
                        .getMetadata()
                        .getTabletMap()
                        .get()
                        .getMapping()
                        .containsKey(TABLET_MAP_KEY));
    // Reset invocations for the next test method
    Mockito.clearInvocations(listener);
  }

  @Test
  public void should_remove_tablets_on_keyspace_update() {
    SESSION_RULE
        .session()
        .execute("ALTER KEYSPACE " + KEYSPACE_NAME + " WITH durable_writes = false");
    ArgumentCaptor<KeyspaceMetadata> previous = ArgumentCaptor.forClass(KeyspaceMetadata.class);
    Mockito.verify(listener, Mockito.timeout(NOTIF_TIMEOUT_MS).times(1))
        .onKeyspaceUpdated(Mockito.any(), previous.capture());
    assertThat(previous.getValue().getName()).isEqualTo(CqlIdentifier.fromCql(KEYSPACE_NAME));
    Assert.assertTrue(SESSION_RULE.session().getMetadata().getTabletMap().isPresent());
    assertThat(SESSION_RULE.session().getMetadata().getTabletMap().get().getMapping().keySet())
        .doesNotContain(TABLET_MAP_KEY);
  }

  @Test
  public void should_remove_tablets_on_keyspace_drop() {
    SESSION_RULE.session().execute(DROP_KEYSPACE);
    ArgumentCaptor<KeyspaceMetadata> keyspace = ArgumentCaptor.forClass(KeyspaceMetadata.class);
    Mockito.verify(listener, Mockito.timeout(NOTIF_TIMEOUT_MS).times(1))
        .onKeyspaceDropped(keyspace.capture());
    assertThat(keyspace.getValue().getName()).isEqualTo(CqlIdentifier.fromCql(KEYSPACE_NAME));
    Assert.assertTrue(SESSION_RULE.session().getMetadata().getTabletMap().isPresent());
    assertThat(SESSION_RULE.session().getMetadata().getTabletMap().get().getMapping().keySet())
        .doesNotContain(TABLET_MAP_KEY);
  }

  @Test
  public void should_remove_tablets_on_table_update() {
    SESSION_RULE
        .session()
        .execute("ALTER TABLE " + KEYSPACE_NAME + "." + TABLE_NAME + " ADD anotherCol int");
    ArgumentCaptor<TableMetadata> previous = ArgumentCaptor.forClass(TableMetadata.class);
    Mockito.verify(listener, Mockito.timeout(NOTIF_TIMEOUT_MS).times(1))
        .onTableUpdated(Mockito.any(), previous.capture());
    assertThat(previous.getValue().getName()).isEqualTo(CqlIdentifier.fromCql(TABLE_NAME));
    Assert.assertTrue(SESSION_RULE.session().getMetadata().getTabletMap().isPresent());
    assertThat(SESSION_RULE.session().getMetadata().getTabletMap().get().getMapping().keySet())
        .doesNotContain(TABLET_MAP_KEY);
  }

  @Test
  public void should_remove_tablets_on_table_drop() {
    SESSION_RULE.session().execute("DROP TABLE " + KEYSPACE_NAME + "." + TABLE_NAME);
    ArgumentCaptor<TableMetadata> table = ArgumentCaptor.forClass(TableMetadata.class);
    Mockito.verify(listener, Mockito.timeout(NOTIF_TIMEOUT_MS).times(1))
        .onTableDropped(table.capture());
    assertThat(table.getValue().getName()).isEqualTo(CqlIdentifier.fromCql(TABLE_NAME));
    Assert.assertTrue(SESSION_RULE.session().getMetadata().getTabletMap().isPresent());
    assertThat(SESSION_RULE.session().getMetadata().getTabletMap().get().getMapping().keySet())
        .doesNotContain(TABLET_MAP_KEY);
  }
}
