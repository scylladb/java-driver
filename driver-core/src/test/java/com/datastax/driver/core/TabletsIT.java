package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.utils.ScyllaOnly;
import com.datastax.driver.core.utils.ScyllaSkip;
import java.nio.ByteBuffer;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

@CCMConfig(
    numberOfNodes = 1,
    jvmArgs = {
      "--experimental-features=consistent-topology-changes",
      "--experimental-features=tablets"
    })
@ScyllaOnly
@ScyllaSkip // There is no released version with tablets-routing-v1 currently
public class TabletsIT extends CCMTestsSupport {

  private static final int INITIAL_TABLETS = 32;
  private static final int QUERIES = 400;
  private static String KEYSPACE_NAME = "tabletsTest";
  private static String TABLE_NAME = "tabletsTable";
  private static String CREATE_KEYSPACE_QUERY_V2 =
      "CREATE KEYSPACE "
          + KEYSPACE_NAME
          + " WITH replication = {'class': "
          + "'NetworkTopologyStrategy', "
          + "'replication_factor': '1'}  AND durable_writes = true AND tablets = "
          + "{'initial': "
          + INITIAL_TABLETS
          + "};";
  private static String CREATE_KEYSPACE_QUERY_V1 =
      "CREATE KEYSPACE "
          + KEYSPACE_NAME
          + " WITH replication = {'class': "
          + "'NetworkTopologyStrategy', "
          + "'replication_factor': '1', 'initial_tablets': '"
          + INITIAL_TABLETS
          + "'}  AND durable_writes = true;";
  private static String CREATE_TABLE_QUERY =
      "CREATE TABLE "
          + KEYSPACE_NAME
          + "."
          + TABLE_NAME
          + " (pk int, ck int, PRIMARY KEY(pk, ck));";

  @Test(groups = "short")
  public void testTabletsRoutingV1() throws InterruptedException {
    try {
      session().execute(CREATE_KEYSPACE_QUERY_V2);
    } catch (SyntaxError ex) {
      if (ex.getMessage().contains("Unknown property 'tablets'")) {
        session().execute(CREATE_KEYSPACE_QUERY_V1);
      } else {
        throw ex;
      }
    }

    session().execute(CREATE_TABLE_QUERY);

    for (int i = 1; i <= QUERIES; i++) {
      session()
          .execute(
              "INSERT INTO "
                  + KEYSPACE_NAME
                  + "."
                  + TABLE_NAME
                  + " (pk,ck) VALUES ("
                  + i
                  + ","
                  + i
                  + ");");
    }

    PreparedStatement preparedStatement =
        session()
            .prepare(
                "select pk,ck from "
                    + KEYSPACE_NAME
                    + "."
                    + TABLE_NAME
                    + " WHERE pk = ? AND ck = ?");
    preparedStatement.enableTracing();
    int counter = 0;
    for (int i = 1; i <= QUERIES; i++) {
      ResultSet rs = session().execute(preparedStatement.bind(i, i));
      Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();
      if (payload != null
          && payload.containsKey(
              TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY)) { // We hit wrong tablet
        counter++;
      }
    }

    assertThat(counter).isEqualTo(INITIAL_TABLETS);

    // All tablet information should be available by now (unless for some reason cluster did sth on
    // its own)
    // We should not receive any tablet payloads now, since they are sent only on mismatch.
    for (int i = 1; i <= QUERIES; i++) {
      ResultSet rs = session().execute(preparedStatement.bind(i, i));
      Map<String, ByteBuffer> payload = rs.getExecutionInfo().getIncomingPayload();
      if (payload != null
          && payload.containsKey(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY)) {
        Assert.fail("Received non empty payload with tablets routing information: " + payload);
      }
    }
  }
}
