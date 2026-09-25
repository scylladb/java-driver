package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.Metadata.handleId;
import static org.awaitility.Awaitility.await;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.utils.ScyllaVersion;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CCMConfig(
    createCluster = false,
    numberOfNodes = 2,
    jvmArgs = {
      "--experimental-features=consistent-topology-changes",
      "--experimental-features=tablets"
    })
@ScyllaVersion(minOSS = "6.0.0", minEnterprise = "2024.2", description = "Needs to support tablets")
public class TabletMapListenerTest extends CCMTestsSupport {
  private static final int INITIAL_TABLETS = 32;
  private static final String KEYSPACE_NAME = "listenerTest";
  private static final String TABLE_NAME = "testTable";
  private static final String CREATE_TABLETS_KEYSPACE_QUERY =
      "CREATE KEYSPACE "
          + KEYSPACE_NAME
          + " WITH replication = {'class': "
          + "'NetworkTopologyStrategy', "
          + "'replication_factor': '1'}  AND durable_writes = true AND tablets = "
          + "{'initial': "
          + INITIAL_TABLETS
          + "};";

  private static final String CREATE_KEYSPACE = CREATE_TABLETS_KEYSPACE_QUERY;
  private static final String ALTER_KEYSPACE =
      "ALTER KEYSPACE " + KEYSPACE_NAME + " WITH durable_writes = false";
  private static final String DROP_KEYSPACE = "DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME;

  private static final String CREATE_TABLE =
      "CREATE TABLE " + KEYSPACE_NAME + "." + TABLE_NAME + "(i int primary key)";
  private static final String INSERT_QUERY_TEMPLATE =
      "INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (i) VALUES (%s)";
  private static final String INSERT_ALTERED_TEMPLATE =
      "INSERT INTO " + KEYSPACE_NAME + "." + TABLE_NAME + " (i,j) VALUES (%s,%s)";
  private static final String SELECT_PK_WHERE =
      "SELECT i FROM " + KEYSPACE_NAME + "." + TABLE_NAME + " WHERE  i = ?";
  private static final String ALTER_TABLE =
      "ALTER TABLE " + KEYSPACE_NAME + "." + TABLE_NAME + " ADD j int";
  private static final String DROP_TABLE = "DROP TABLE " + KEYSPACE_NAME + "." + TABLE_NAME;
  private static final TabletMap.KeyspaceTableNamePair TABLET_MAP_KEY =
      new TabletMap.KeyspaceTableNamePair(handleId(KEYSPACE_NAME), handleId(TABLE_NAME));

  /** The maximum time that the test will wait to check that listeners have been notified. */
  private static final long NOTIF_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
  /** Shorter timeout for less important checks that listeners did not react to specific actions */
  private static final long SHORT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(4);

  Cluster cluster;
  Session session;
  SchemaChangeListener listener;
  List<SchemaChangeListener> listeners;

  @BeforeMethod(groups = "short")
  public void setup() throws InterruptedException {
    cluster =
        createClusterBuilderNoDebouncing().withLoadBalancingPolicy(new RoundRobinPolicy()).build();

    session = cluster.connect();

    cluster.register(listener = mock(TabletMapListener.class));
    listeners = Lists.newArrayList(listener);
  }

  // Checks for tablet removal both on table update and removal
  @Test(groups = "short")
  public void should_remove_tablets_on_table_alterations() throws InterruptedException {
    session.execute(CREATE_KEYSPACE);
    ArgumentCaptor<KeyspaceMetadata> added = null;
    for (SchemaChangeListener listener : listeners) {
      added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
      verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
      assertThat(added.getValue()).hasName(handleId(KEYSPACE_NAME));
    }
    assert added != null;

    TabletMap tabletMap;
    tabletMap = cluster.getMetadata().getTabletMap();

    session.execute(CREATE_TABLE);
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> !tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    session.execute(String.format(INSERT_QUERY_TEMPLATE, "42"));
    executeOnAllHosts(session.prepare(SELECT_PK_WHERE).bind(42), session);
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    session.execute(ALTER_TABLE);
    for (SchemaChangeListener listener : listeners) {
      ArgumentCaptor<TableMetadata> current = ArgumentCaptor.forClass(TableMetadata.class);
      ArgumentCaptor<TableMetadata> previous = ArgumentCaptor.forClass(TableMetadata.class);
      verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1))
          .onTableChanged(current.capture(), previous.capture());
      assertThat(previous.getValue().getKeyspace()).hasName(handleId(KEYSPACE_NAME));
      assertThat(previous.getValue()).hasName(handleId(TABLE_NAME));
    }
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> !tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    session.execute(String.format(INSERT_ALTERED_TEMPLATE, "42", "42"));
    executeOnAllHosts(session.prepare(SELECT_PK_WHERE).bind(42), session);
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    session.execute(DROP_TABLE);
    ArgumentCaptor<TableMetadata> removed = null;
    for (SchemaChangeListener listener : listeners) {
      removed = ArgumentCaptor.forClass(TableMetadata.class);
      verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onTableRemoved(removed.capture());
      assertThat(removed.getValue().getKeyspace()).hasName(handleId(KEYSPACE_NAME));
      assertThat(removed.getValue()).hasName(handleId(TABLE_NAME));
    }
    assertThat(removed).isNotNull();
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> !tabletMap.getMapping().containsKey(TABLET_MAP_KEY));
  }

  @Test(groups = "short")
  public void should_remove_tablets_on_keyspace_alterations() {
    session.execute(CREATE_KEYSPACE);
    ArgumentCaptor<KeyspaceMetadata> added = null;
    for (SchemaChangeListener listener : listeners) {
      added = ArgumentCaptor.forClass(KeyspaceMetadata.class);
      verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceAdded(added.capture());
      assertThat(added.getValue()).hasName(handleId(KEYSPACE_NAME));
    }
    assert added != null;

    TabletMap tabletMap;
    tabletMap = cluster.getMetadata().getTabletMap();

    session.execute(CREATE_TABLE);
    session.execute(String.format(INSERT_QUERY_TEMPLATE, "42"));
    executeOnAllHosts(session.prepare(SELECT_PK_WHERE).bind(42), session);
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    session.execute(ALTER_KEYSPACE);
    assertThat(cluster.getMetadata().getKeyspace(KEYSPACE_NAME)).isNotDurableWrites();
    for (SchemaChangeListener listener : listeners) {
      ArgumentCaptor<KeyspaceMetadata> current = ArgumentCaptor.forClass(KeyspaceMetadata.class);
      ArgumentCaptor<KeyspaceMetadata> previous = ArgumentCaptor.forClass(KeyspaceMetadata.class);
      verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1))
          .onKeyspaceChanged(current.capture(), previous.capture());
      assertThat(previous.getValue()).hasName(handleId(KEYSPACE_NAME));
    }
    for (SchemaChangeListener listener : listeners) {
      verify(listener, after((int) SHORT_TIMEOUT_MS).never())
          .onTableChanged(anyObject(), anyObject());
    }
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> !tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    executeOnAllHosts(session.prepare(SELECT_PK_WHERE).bind(42), session);
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> tabletMap.getMapping().containsKey(TABLET_MAP_KEY));

    session.execute(DROP_KEYSPACE);
    for (SchemaChangeListener listener : listeners) {
      ArgumentCaptor<KeyspaceMetadata> removed = ArgumentCaptor.forClass(KeyspaceMetadata.class);
      verify(listener, timeout(NOTIF_TIMEOUT_MS).times(1)).onKeyspaceRemoved(removed.capture());
      assertThat(removed.getValue()).hasName(handleId(KEYSPACE_NAME));
    }
    await()
        .atMost(SHORT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .until(() -> !tabletMap.getMapping().containsKey(TABLET_MAP_KEY));
  }

  @AfterMethod(groups = "short", alwaysRun = true)
  public void teardown() {
    if (session != null) {
      session.execute(DROP_KEYSPACE);
      session.close();
    }
    if (cluster != null) cluster.close();
  }

  private void executeOnAllHosts(Statement statement, Session session) {
    for (Host host : session.getCluster().getMetadata().getAllHosts()) {
      session.execute(statement.setHost(host));
    }
  }
}
