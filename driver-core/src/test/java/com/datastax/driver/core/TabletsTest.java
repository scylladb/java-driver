package com.datastax.driver.core;

import com.datastax.driver.core.utils.ScyllaOnly;
import com.datastax.driver.core.utils.ScyllaVersion;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@CCMConfig(
    numberOfNodes = 3,
    jvmArgs = {
      "--experimental-features=consistent-topology-changes",
      "--experimental-features=tablets"
    })
@ScyllaOnly
@ScyllaVersion(minOSS = "6.0.0", minEnterprise = "2024.2", description = "Needs to support tablets")
public class TabletsTest extends CCMTestsSupport {
  private static final Logger LOG = LoggerFactory.getLogger(TabletsTest.class);
  private static final int INITIAL_TABLETS = 32;
  private static final int QUERIES = 1600;
  private static final int REPLICATION_FACTOR = 2;
  private static final String KEYSPACE_NAME = "tabletsTest";
  private static final String TABLE_NAME = "tabletsTable";
  private static final String DROP_KEYSPACE_QUERY = "DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME;
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
          + " (pk int, ck int, val int, PRIMARY KEY(pk, ck));";

  private static final SimpleStatement STMT_INSERT =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (?, ?);");

  private static final SimpleStatement STMT_INSERT_NO_KS =
      buildStatement("INSERT INTO %s (pk, ck) VALUES (?, ?);");

  private static final SimpleStatement STMT_INSERT_CONCRETE =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (1, 1);");

  private static final SimpleStatement STMT_INSERT_PK_CONCRETE =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (1, ?);");

  private static final SimpleStatement STMT_INSERT_CK_CONCRETE =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (?, 1);");

  private static final SimpleStatement STMT_INSERT_LWT_IF_NOT_EXISTS =
      buildStatement("INSERT INTO %s.%s (pk, ck) VALUES (?, ?) IF NOT EXISTS;");

  private static final SimpleStatement STMT_SELECT =
      buildStatement("SELECT pk,ck FROM %s.%s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_SELECT_NO_KS =
      buildStatement("SELECT pk, ck FROM %s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_SELECT_CONCRETE =
      buildStatement("SELECT pk,ck FROM %s.%s WHERE pk = 1 AND ck = 1");

  private static final SimpleStatement STMT_SELECT_PK_CONCRETE =
      buildStatement("SELECT pk, ck FROM %s.%s WHERE pk = 1 AND ck = ?");

  private static final SimpleStatement STMT_SELECT_CK_CONCRETE =
      buildStatement("SELECT pk, ck FROM %s.%s WHERE pk = ? AND ck = 1");

  private static final SimpleStatement STMT_UPDATE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_UPDATE_NO_KS =
      buildStatement("UPDATE %s SET val = 1 WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_UPDATE_CONCRETE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = 1 AND ck = 1");

  private static final SimpleStatement STMT_UPDATE_PK_CONCRETE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = 1 AND ck = ?");

  private static final SimpleStatement STMT_UPDATE_CK_CONCRETE =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = 1");

  private static final SimpleStatement STMT_UPDATE_LWT_IF_EXISTS =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = ? IF EXISTS");

  private static final SimpleStatement STMT_UPDATE_LWT_IF_VAL =
      buildStatement("UPDATE %s.%s SET val = 1 WHERE pk = ? AND ck = ? IF val = 2");

  private static final SimpleStatement STMT_DELETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_DELETE_NO_KS =
      buildStatement("DELETE FROM %s WHERE pk = ? AND ck = ?");

  private static final SimpleStatement STMT_DELETE_CONCRETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = 1 AND ck = 1");

  private static final SimpleStatement STMT_DELETE_PK_CONCRETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = 1 AND ck = ?");

  private static final SimpleStatement STMT_DELETE_CK_CONCRETE =
      buildStatement("DELETE FROM %s.%s WHERE pk = ? AND ck = 1");

  private static final SimpleStatement STMT_DELETE_IF_EXISTS =
      buildStatement("DELETE FROM %s.%s WHERE pk = ? AND ck = ? IF EXISTS");

  public void prepareCluster() {
    session().execute(DROP_KEYSPACE_QUERY);
    session().execute(CREATE_KEYSPACE_QUERY);
    session().execute(CREATE_TABLE_QUERY);
  }

  @Test(groups = "short")
  public void every_statement_should_deliver_tablet_info() {
    prepareCluster();

    Map<String, Supplier<Session>> sessions = new HashMap<>();
    sessions.put("REGULAR", this::newSession);
    sessions.put(
        "USE_KEYSPACE",
        () -> {
          Session s = newSession();
          s.execute("USE " + KEYSPACE_NAME);
          return s;
        });

    Map<String, Function<Session, Statement>> statements = new HashMap<>();
    statements.put("SELECT_CONCRETE", s -> STMT_SELECT_CONCRETE);
    statements.put("SELECT_PREPARED", s -> s.prepare(STMT_SELECT).bind(2, 2));
    statements.put("SELECT_NO_KS_PREPARED", s -> s.prepare(STMT_SELECT_NO_KS).bind(2, 2));
    statements.put("SELECT_CONCRETE_PREPARED", s -> s.prepare(STMT_SELECT_CONCRETE).bind());
    statements.put("SELECT_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_SELECT_PK_CONCRETE).bind(2));
    statements.put("SELECT_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_SELECT_CK_CONCRETE).bind(2));
    statements.put("INSERT_CONCRETE", s -> STMT_INSERT_CONCRETE);
    statements.put("INSERT_PREPARED", s -> s.prepare(STMT_INSERT).bind(2, 2));
    statements.put("INSERT_NO_KS_PREPARED", s -> s.prepare(STMT_INSERT_NO_KS).bind(2, 2));
    statements.put("INSERT_CONCRETE_PREPARED", s -> s.prepare(STMT_INSERT_CONCRETE).bind());
    statements.put("INSERT_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_INSERT_PK_CONCRETE).bind(2));
    statements.put("INSERT_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_INSERT_CK_CONCRETE).bind(2));
    statements.put(
        "INSERT_LWT_IF_NOT_EXISTS", s -> s.prepare(STMT_INSERT_LWT_IF_NOT_EXISTS).bind(2, 2));
    statements.put("UPDATE_CONCRETE", s -> STMT_UPDATE_CONCRETE);
    statements.put("UPDATE_PREPARED", s -> s.prepare(STMT_UPDATE).bind(2, 2));
    statements.put("UPDATE_NO_KS_PREPARED", s -> s.prepare(STMT_UPDATE_NO_KS).bind(2, 2));
    statements.put("UPDATE_CONCRETE_PREPARED", s -> s.prepare(STMT_UPDATE_CONCRETE).bind());
    statements.put("UPDATE_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_UPDATE_PK_CONCRETE).bind(2));
    statements.put("UPDATE_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_UPDATE_CK_CONCRETE).bind(2));
    statements.put("UPDATE_LWT_IF_EXISTS", s -> s.prepare(STMT_UPDATE_LWT_IF_EXISTS).bind(2, 2));
    statements.put("STMT_UPDATE_LWT_IF_VAL", s -> s.prepare(STMT_UPDATE_LWT_IF_VAL).bind(2, 2));
    statements.put("DELETE_CONCRETE", s -> STMT_DELETE_CONCRETE);
    statements.put("DELETE_PREPARED", s -> s.prepare(STMT_DELETE).bind(2, 2));
    statements.put("DELETE_NO_KS_PREPARED", s -> s.prepare(STMT_DELETE_NO_KS).bind(2, 2));
    statements.put("DELETE_CONCRETE_PREPARED", s -> s.prepare(STMT_DELETE_CONCRETE).bind());
    statements.put("DELETE_PK_CONCRETE_PREPARED", s -> s.prepare(STMT_DELETE_PK_CONCRETE).bind(2));
    statements.put("DELETE_CK_CONCRETE_PREPARED", s -> s.prepare(STMT_DELETE_CK_CONCRETE).bind(2));
    statements.put("DELETE_LWT_IF_EXISTS", s -> s.prepare(STMT_DELETE_IF_EXISTS).bind(2, 2));

    List<String> testErrors = new ArrayList<>();
    for (Map.Entry<String, Supplier<Session>> sessionEntry : sessions.entrySet()) {
      for (Map.Entry<String, Function<Session, Statement>> stmtEntry : statements.entrySet()) {
        if (stmtEntry.getKey().contains("CONCRETE")
            && !stmtEntry.getKey().contains("CK_CONCRETE")) {
          // Scylla does not return tablet info for queries with PK built into query
          continue;
        }
        if (stmtEntry.getKey().contains("LWT")) {
          // LWT is not yet supported by scylla on tables with tablets
          continue;
        }
        if (sessionEntry.getKey().equals("REGULAR") && stmtEntry.getKey().contains("NO_KS")) {
          // Preparation of the statements without KS will fail on the session with no ks specified
          continue;
        }
        Session session = sessionEntry.getValue().get();
        // Empty out tablets information
        session.getCluster().getMetadata().getTabletMap().removeTableMappings(KEYSPACE_NAME);
        Statement stmt;
        try {
          stmt = stmtEntry.getValue().apply(session);
        } catch (Exception e) {
          RuntimeException ex =
              new RuntimeException(
                  String.format(
                      "Failed to build statement %s on session %s",
                      stmtEntry.getKey(), sessionEntry.getKey()));
          ex.addSuppressed(e);
          throw ex;
        }
        try {
          if (!executeOnAllHostsAndReturnIfResultHasTabletsInfo(session, stmt)) {
            testErrors.add(
                String.format(
                    "Statement %s on session %s got no tablet info",
                    stmtEntry.getKey(), sessionEntry.getKey()));
            continue;
          }
        } catch (Exception e) {
          testErrors.add(
              String.format(
                  "Failed to execute statement %s on session %s: %s",
                  stmtEntry.getKey(), sessionEntry.getKey(), e));
          continue;
        }
        if (!waitSessionLearnedTabletInfo(session)) {
          testErrors.add(
              String.format(
                  "Statement %s on session %s did not trigger session tablets update",
                  stmtEntry.getKey(), sessionEntry.getKey()));
          continue;
        }
        if (!checkIfRoutedProperly(session, stmt)) {
          testErrors.add(
              String.format(
                  "Statement %s on session %s was routed to different nodes",
                  stmtEntry.getKey(), sessionEntry.getKey()));
        }
      }
    }

    if (!testErrors.isEmpty()) {
      throw new AssertionError(
          String.format(
              "Found queries that got no tablet info: \n%s", String.join("\n", testErrors)));
    }
  }

  @Test(groups = "short")
  public void should_receive_each_tablet_exactly_once() {
    prepareCluster();
    Session session = newSession();
    session
        .getCluster()
        .getMetadata()
        .getTabletMap()
        .removeTableMappings(KEYSPACE_NAME.toLowerCase());
    int counter = 0;
    PreparedStatement preparedStatement = session.prepare(STMT_INSERT);
    for (int i = 1; i <= QUERIES; i++) {
      if (executeAndReturnIfResultHasTabletsInfo(session, preparedStatement.bind(i, i))) {
        counter++;
      }
    }
    Assert.assertEquals(counter, INITIAL_TABLETS);
    assertSessionTabletMapIsFilled(session);
    session.close();

    session = newSession();
    session
        .getCluster()
        .getMetadata()
        .getTabletMap()
        .removeTableMappings(KEYSPACE_NAME.toLowerCase());
    counter = 0;
    preparedStatement = session.prepare(STMT_SELECT);
    for (int i = 1; i <= QUERIES; i++) {
      if (executeAndReturnIfResultHasTabletsInfo(session, preparedStatement.bind(i, i))) {
        counter++;
      }
    }

    LOG.debug("Ran first set of queries");

    // With enough queries we should hit a wrong node for each tablet exactly once.
    Assert.assertEquals(counter, INITIAL_TABLETS);
    assertSessionTabletMapIsFilled(session);

    // All tablet information should be available by now (unless for some reason cluster did sth on
    // its own)
    // We should not receive any tablet payloads now, since they are sent only on mismatch.
    for (int i = 1; i <= QUERIES; i++) {

      ResultSet rs = session.execute(preparedStatement.bind(i, i));
      Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();

      if (payload != null
          && payload.containsKey(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY)) {
        throw new RuntimeException(
            "Received non empty payload with tablets routing information: " + payload);
      }
    }
  }

  @Test(groups = "short")
  public void batch_statement_should_deliver_tablet_info_and_route_properly() {
    prepareCluster();
    Session session = newSession();
    try {
      session
          .getCluster()
          .getMetadata()
          .getTabletMap()
          .removeTableMappings(KEYSPACE_NAME.toLowerCase());

      PreparedStatement preparedStatement = session.prepare(STMT_INSERT);
      Assert.assertTrue(
          executeOnAllHostsAndReturnIfResultHasTabletsInfo(session, preparedStatement.bind(2, 2)));
      Assert.assertTrue(waitSessionLearnedTabletInfo(session));

      BatchStatement routedBatch = new BatchStatement().add(preparedStatement.bind(2, 2));
      Assert.assertTrue(checkIfRoutedProperly(session, routedBatch));
    } finally {
      session.close();
    }
  }

  private static boolean waitSessionLearnedTabletInfo(Session session) {
    if (isSessionLearnedTabletInfo(session)) {
      return true;
    }
    // Wait till tablet update, which is async, is completed
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return isSessionLearnedTabletInfo(session);
  }

  private static boolean checkIfRoutedProperly(Session session, Statement stmt) {
    // DefaultLoadBalancingPolicy suppose to prioritize nodes from replica list randomly shuffling
    // them
    // If routing goes wrong you will see more than REPLICATION_FACTOR unique first nodes in
    // execution plan

    int expectedNodesCount = stmt.isLWT() ? 1 : REPLICATION_FACTOR;
    Set<Host> nodes = new HashSet<>();
    for (int i = 0; i < REPLICATION_FACTOR * 3; i++) {
      nodes.add(session.execute(stmt).getExecutionInfo().getQueriedHost());
    }
    return nodes.size() <= expectedNodesCount;
  }

  private static boolean isSessionLearnedTabletInfo(Session session) {
    Map<TabletMap.KeyspaceTableNamePair, TabletMap.TabletSet> tabletMapping =
        session.getCluster().getMetadata().getTabletMap().getMapping();
    TabletMap.KeyspaceTableNamePair ktPair =
        new TabletMap.KeyspaceTableNamePair(KEYSPACE_NAME.toLowerCase(), TABLE_NAME.toLowerCase());

    TabletMap.TabletSet tablets = tabletMapping.get(ktPair);
    if (tablets == null || tablets.tablets.isEmpty()) {
      return false;
    }

    for (TabletMap.Tablet tab : tablets.tablets) {
      if (tab.getReplicas().size() >= REPLICATION_FACTOR) {
        return true;
      }
    }
    return false;
  }

  private static void assertSessionTabletMapIsFilled(Session session) {
    Map<TabletMap.KeyspaceTableNamePair, TabletMap.TabletSet> tabletMapping =
        session.getCluster().getMetadata().getTabletMap().getMapping();
    TabletMap.KeyspaceTableNamePair ktPair =
        new TabletMap.KeyspaceTableNamePair(KEYSPACE_NAME.toLowerCase(), TABLE_NAME.toLowerCase());
    Assert.assertTrue(tabletMapping.containsKey(ktPair));

    TabletMap.TabletSet tablets = tabletMapping.get(ktPair);
    Assert.assertEquals(tablets.tablets.size(), INITIAL_TABLETS);

    for (TabletMap.Tablet tab : tablets.tablets) {
      Assert.assertEquals(tab.getReplicas().size(), REPLICATION_FACTOR);
    }
  }

  private static boolean executeOnAllHostsAndReturnIfResultHasTabletsInfo(
      Session session, Statement stmt) {
    for (Host node : session.getCluster().getMetadata().getAllHosts()) {
      if (executeAndReturnIfResultHasTabletsInfo(session, stmt.setHost(node))) {
        return true;
      }
    }
    return false;
  }

  private static boolean executeAndReturnIfResultHasTabletsInfo(
      Session session, Statement statement) {
    ResultSet rs = session.execute(statement);
    Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();
    if (payload == null) {
      return false;
    }
    return payload.containsKey(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY);
  }

  private static SimpleStatement buildStatement(String statement) {
    if (statement.contains("%s.%s")) {
      return new SimpleStatement(String.format(statement, KEYSPACE_NAME, TABLE_NAME));
    }
    return new SimpleStatement(String.format(statement, TABLE_NAME));
  }
}
