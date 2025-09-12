package com.datastax.driver.core;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.utils.ScyllaVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@CreateCCM(PER_METHOD)
@CCMConfig(
    auth = false,
    config = "client_encryption_options.enable_session_tickets:true",
    jvmArgs = {"--smp", "5"},
    dirtiesContext = true)
public class SSLSessionTicketsTest extends SSLTestBase {

  private static final int NUM_SHARDS = 5; // Has to match the smp value above

  private Logger sslLogger;
  private Level originalLevel;
  private TlsDebugLogHandler handler;

  private final OccurrenceCounter serverHellos =
      new OccurrenceCounter("Consuming ServerHello handshake message");
  private final OccurrenceCounter negotiatedTls13 =
      new OccurrenceCounter("Negotiated protocol version: TLSv1.3");
  private final OccurrenceCounter resumptions = new OccurrenceCounter("Resuming session:");
  private final OccurrenceCounter pskUses =
      new OccurrenceCounter("Using PSK to derive early secret");
  private final OccurrenceCounter ticketsReceived =
      new OccurrenceCounter("Consuming NewSessionTicket");
  private final List<OccurrenceCounter> counters =
      ImmutableList.of(serverHellos, resumptions, pskUses, ticketsReceived, negotiatedTls13);

  /**
   * @test_category connection:ssl
   * @expected_result Connection can be established.
   */
  @Test(groups = "isolated")
  @ScyllaVersion(
      minEnterprise = "2025.2.0",
      maxOSS = "0.0.0",
      description = "Requires certain options to be enabled server side. Since scylladb/pull/22928")
  public void should_receive_tickets_TLSv13_JDK() throws Exception {
    try {
      setupJavaSslLogTracking();
      SSLOptions sslOptions = getSSLOptions(SslImplementation.JDK, false, true, "TLSv1.3");
      Cluster cluster = register(createClusterBuilder().withSSL(sslOptions).build());
      Session session = cluster.connect();
      ResultSet rs = session.execute("SELECT * FROM system.local");
      healthCheck(session);
      assertEquals(
          negotiatedTls13.get(), serverHellos.get(), "Every negotiated TLS version should be 1.3");
      assertTrue(ticketsReceived.get() > 0, "Client should have received some tickets");
      // If server ever starts sending less (or more) tickets this check below will alert us
      assertEquals(
          ticketsReceived.get(), serverHellos.get() * 2, "We expect 2 tickets per connection");
      assertTrue(resumptions.get() > 0, "Client should have resumed at least one session");
      assertTrue(pskUses.get() > 0, "Client should have used PSK at least once for the resumption");
    } finally {
      cleanUpJavaSslLogTracking();
    }
  }

  @Test(groups = "isolated")
  @ScyllaVersion(
      minEnterprise = "2025.2.0",
      maxOSS = "0.0.0",
      description = "Requires certain options to be enabled server side. Since scylladb/pull/22928")
  public void all_reconnections_should_use_tickets_TLSv13_netty() throws Exception {
    TestableNettySSLOptions testableSSLOptions =
        (TestableNettySSLOptions)
            getSSLOptions(SslImplementation.NETTY_OPENSSL_DEBUG, false, true, "TLSv1.3");

    testableSSLOptions.resetCounters();
    Cluster cluster =
        register(
            createClusterBuilder()
                .withSSL(testableSSLOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(200))
                .build());
    Session session = cluster.connect();
    ResultSet rs = session.execute("SELECT * FROM system.local");
    healthCheck(session);

    ccm().stop(1);
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
    ccm().start(1);
    healthCheck(session);

    // Assert that every connection negotiated TLS 1.3
    assertEquals(
        testableSSLOptions.getTls13Negotiations(),
        testableSSLOptions.getHandshakeCompletions(),
        "Every " + "negotiated TLS version should always be 1.3");

    // Assert that last <expectedConnections> of ClientHellos contained unique PSK identities
    int expectedConnections =
        getExpectedNumberOfConnectionsPerHost(session) + 1; // +1 for the control connection
    List<TestableNettySSLOptions.ClientHelloInfo> clientHelloHistory =
        testableSSLOptions.getClientHelloHistory();
    List<TestableNettySSLOptions.ClientHelloInfo> lastClientHellos =
        clientHelloHistory.subList(
            clientHelloHistory.size() - expectedConnections, clientHelloHistory.size());
    // Assert that every element in this list has a psk identity list of 1
    long pskIdentityListsOfSize1 =
        lastClientHellos.stream().filter(c -> c.getPreSharedKeys().size() == 1).count();
    // Technically the client could send more than 1 PSK identity. It would be unexpected here
    // though.
    assertEquals(
        pskIdentityListsOfSize1,
        expectedConnections,
        "All final ClientHellos should have a PSK identity list of size 1");
    // Assert that every element in this list has a unique PSK identity
    long uniquePskIdentities =
        lastClientHellos.stream()
            .map(c -> c.getPreSharedKeys().get(0).getIdentity())
            .distinct()
            .count();
    assertEquals(
        uniquePskIdentities,
        expectedConnections,
        "Every final connection should have utilized PSK to resume the session");
  }

  @Test(
      groups = "isolated",
      expectedExceptions = AssertionError.class,
      expectedExceptionsMessageRegExp = ".*Every reconnection should be a resumption.*")
  @ScyllaVersion(
      minEnterprise = "2025.2.0",
      maxOSS = "0.0.0",
      description = "Requires certain options to be enabled server side. Since scylladb/pull/22928")
  public void all_reconnections_should_use_tickets_TLSv13_JDK() throws Exception {
    // Unfortunately the OpenJDK's cache in older versions cannot hold more than 1 ticket
    // making the reconnection scenario with all reconnections using tickets impossible.
    // For additional context see https://github.com/scylladb/java-driver/issues/444
    // The insights on what's happening on JDK side should be still relevant despite
    // different driver version
    int initialResumptions, reconnectionResumptions;
    int initialHellos, reconnectionHellos;
    int initialPsks, reconnectionPsks;
    try {
      setupJavaSslLogTracking();
      SSLOptions sslOptions = getSSLOptions(SslImplementation.JDK, false, true, "TLSv1.3");
      Cluster cluster = register(createClusterBuilder().withSSL(sslOptions).build());
      Session session = cluster.connect();
      ResultSet rs = session.execute("SELECT * FROM system.local");
      healthCheck(session);
      initialResumptions = resumptions.get();
      initialHellos = serverHellos.get();
      initialPsks = pskUses.get();
      ccm().stop(1);
      Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
      ccm().start(1);
      healthCheck(session);
      reconnectionResumptions = resumptions.get() - initialResumptions;
      reconnectionHellos = serverHellos.get() - initialHellos;
      reconnectionPsks = pskUses.get() - initialPsks;
      assertEquals(
          negotiatedTls13.get(), serverHellos.get(), "Every negotiated TLS version should be 1.3");
      assertTrue(ticketsReceived.get() > 0, "Client should have received some tickets");
      assertEquals(
          reconnectionResumptions, reconnectionHellos, "Every reconnection should be a resumption");
      assertEquals(
          reconnectionPsks, reconnectionHellos, "Every reconnection resumption should use PSK");
    } finally {
      cleanUpJavaSslLogTracking();
    }
  }

  public void setupJavaSslLogTracking() {
    System.setProperty("javax.net.debug", "");
    sslLogger = Logger.getLogger("javax.net.ssl");
    originalLevel = sslLogger.getLevel();
    sslLogger.setLevel(Level.ALL);

    for (OccurrenceCounter counter : counters) {
      counter.reset();
    }

    // Custom handler to capture log messages
    ByteArrayOutputStream logCapture = new ByteArrayOutputStream();
    handler = new TlsDebugLogHandler(logCapture, counters);
    sslLogger.setUseParentHandlers(false);
    sslLogger.addHandler(handler);
  }

  public void cleanUpJavaSslLogTracking() {
    sslLogger.removeHandler(handler);
    sslLogger.setLevel(originalLevel);
  }

  private void healthCheck(Session session) {
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                for (Host host : session.getCluster().getMetadata().getAllHosts()) {
                  int expectedConnections = getExpectedNumberOfConnectionsPerHost(session);
                  if (session.getState().getOpenConnections(host) != expectedConnections) {
                    return false;
                  }
                }
                for (int i = 0; i < 3; i++) {
                  session.execute("select * from system.local where key='local'");
                }
                return true;
              } catch (Exception e) {
                return false;
              }
            });
  }

  private int getExpectedNumberOfConnectionsPerHost(Session session) {
    // In this test we care only about LOCAL connections. There should be no remote connections.
    int expectedConnections =
        session
            .getCluster()
            .getConfiguration()
            .getPoolingOptions()
            .getCoreConnectionsPerHost(HostDistance.LOCAL);
    if (expectedConnections % NUM_SHARDS > 0) {
      expectedConnections += NUM_SHARDS - (expectedConnections % NUM_SHARDS);
    }
    return expectedConnections;
  }

  static class TlsDebugLogHandler extends Handler {
    private final ByteArrayOutputStream outputStream;
    private final List<OccurrenceCounter> counters;

    TlsDebugLogHandler(ByteArrayOutputStream outputStream, List<OccurrenceCounter> counters) {
      this.outputStream = outputStream;
      this.counters = counters;
    }

    @Override
    public void publish(LogRecord record) {
      try {
        for (OccurrenceCounter counter : counters) {
          counter.incrementIfFound(record.getMessage());
        }
        outputStream.write((record.getMessage() + "\n").getBytes(UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void flush() {
      try {
        outputStream.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws SecurityException {
      try {
        outputStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class OccurrenceCounter {
    private final AtomicInteger count = new AtomicInteger(0);
    private final String substring; // Exact substring to look for

    public OccurrenceCounter(String substring) {
      this.substring = substring;
    }

    /**
     * Increment the counter if the substring is found in the log line. Multiple occurrences count
     * as one.
     *
     * @param logLine log line to check
     */
    public void incrementIfFound(String logLine) {
      if (logLine.contains(substring)) {
        count.incrementAndGet();
      }
    }

    public int get() {
      return count.get();
    }

    public String getSubstring() {
      return substring;
    }

    public void reset() {
      count.set(0);
    }

    @Override
    public String toString() {
      return "OccurrenceCounter{substring='" + substring + "', count=" + count.get() + "}";
    }
  }
}
