package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.testng.annotations.Test;

public class StatementPagesTest extends CCMTestsSupport {
  @Override
  public void onTestContextInitialized() {
    execute(
        "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    execute("CREATE TABLE IF NOT EXISTS ks.t (pk int, ck int, v int, PRIMARY KEY(pk, ck))");

    for (int i = 0; i < 50; i++) {
      // System.out.println("Insert " + i);
      session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", i, i, 15);
      session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", i, i, 32);
    }

    session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", 8, 8, 14);
    session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", 11, 11, 14);
    session().execute("INSERT INTO ks.t(pk, ck, v) VALUES (?, ?, ?)", 14, 14, 14);
  }

  @Test(groups = "short")
  public void should_not_get_stuck_on_empty_pages() {
    SimpleStatement st = new SimpleStatement("SELECT * FROM ks.t WHERE v = 14 ALLOW FILTERING");
    st.setFetchSize(1);
    ResultSet rs = session().execute(st);
    assertThat(rs.all().size()).isEqualTo(3);
  }
}
