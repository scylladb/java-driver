package com.datastax.oss.driver.core.ssl;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IsolatedTests.class)
@BackendRequirement(
    type = BackendType.SCYLLA,
    minInclusive = "2025.2.0",
    description = "Requires server side support for session tickets")
public class SessionTicketsIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder()
          .withSsl()
          .withCassandraConfiguration("client_encryption_options.enable_session_tickets", "true")
          .withJvmArgs("--smp=4")
          .build();

  private static final int smpValue = 4;
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

  @Before
  public void setupLogTracking() {
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

  @After
  public void cleanUp() {
    sslLogger.removeHandler(handler);
    sslLogger.setLevel(originalLevel);
  }

  @Test
  public void is_able_to_use_session_tickets_with_TLSv13() throws Exception {
    try {
      SSLContext context = createSslContext("TLSv1.3");
      try (DriverConfigLoader configLoader =
              SessionUtils.configLoaderBuilder()
                  .withString(
                      DefaultDriverOption.PROTOCOL_VERSION, DefaultProtocolVersion.V4.name())
                  .build();
          CqlSession session =
              (CqlSession)
                  SessionUtils.baseBuilder()
                      .addContactEndPoints(CCM_RULE.getContactPoints())
                      .withSslContext(context)
                      .withConfigLoader(configLoader)
                      .build()) {
        healthCheck(session);
      }
    } finally {
      handler.flush();
    }
    Assert.assertEquals(
        "Each connection should have negotiated TLSv1.3",
        serverHellos.get(),
        negotiatedTls13.get());
    Assert.assertTrue("Client should have received some tickets", ticketsReceived.get() > 0);
    Assert.assertTrue("There should be at least one resumption attempt", resumptions.get() > 0);
    Assert.assertTrue(
        "PSK should have been used at least once (for resumption)", pskUses.get() > 0);
  }

  @Test(expected = AssertionError.class)
  // We want all reconnections to use session tickets for resumptions, but Java's
  // handling of TLS sessions has limitations.
  // For details see https://github.com/scylladb/java-driver/issues/444
  // In short before OpenJDK 24 simultaneous session resumption is not supported.
  // Since JDK 24 it is but the session cache size is limited and tickets are single use
  // which means that reconnecting to DOWN node uses up those limited tickets.
  public void all_reconnections_should_use_tickets_with_TLSv13() throws Exception {
    int initialResumptions, reconnectionResumptions;
    int initialHellos, reconnectionHellos;
    int initialPsks, reconnectionPsks;
    try {
      SSLContext context = createSslContext("TLSv1.3");
      try (DriverConfigLoader configLoader =
              SessionUtils.configLoaderBuilder()
                  .withString(
                      DefaultDriverOption.PROTOCOL_VERSION, DefaultProtocolVersion.V4.name())
                  .build();
          CqlSession session =
              (CqlSession)
                  SessionUtils.baseBuilder()
                      .addContactEndPoints(CCM_RULE.getContactPoints())
                      .withSslContext(context)
                      .withConfigLoader(configLoader)
                      .build()) {
        healthCheck(session);
        initialResumptions = resumptions.get();
        initialHellos = serverHellos.get();
        initialPsks = pskUses.get();
        // Perform a node restart to force all connections to be re-established
        CCM_RULE.getCcmBridge().stop();
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
        CCM_RULE.getCcmBridge().start();
        healthCheck(session);
        reconnectionResumptions = resumptions.get() - initialResumptions;
        reconnectionHellos = serverHellos.get() - initialHellos;
        reconnectionPsks = pskUses.get() - initialPsks;
      }
    } finally {
      handler.flush();
    }

    Assert.assertEquals(
        "Each connection should have negotiated TLSv1.3",
        serverHellos.get(),
        negotiatedTls13.get());
    Assert.assertTrue("Client should have received some tickets", ticketsReceived.get() > 0);
    Assert.assertEquals(
        "Each reconnection should be a resumption.", reconnectionHellos, reconnectionResumptions);
    Assert.assertEquals(
        "PSK should have been used for each resumption on reconnection",
        reconnectionHellos,
        reconnectionPsks);
  }

  @Test
  public void all_reconnections_but_one_should_use_tickets_when_throttled_TLSv13()
      throws Exception {
    int initialResumptions, reconnectionResumptions;
    int initialHellos, reconnectionHellos;
    int initialPsks, reconnectionPsks;
    try {
      SSLContext context = createSslContext("TLSv1.3");
      try (DriverConfigLoader configLoader =
              SessionUtils.configLoaderBuilder()
                  .withString(
                      DefaultDriverOption.PROTOCOL_VERSION, DefaultProtocolVersion.V4.name())
                  .withClass(
                      DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                      ConstantReconnectionPolicy.class)
                  .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1))
                  .withInt(DefaultDriverOption.CONNECTION_POOL_INIT_BATCH_SIZE, 1)
                  .build();
          CqlSession session =
              (CqlSession)
                  SessionUtils.baseBuilder()
                      .addContactEndPoints(CCM_RULE.getContactPointsWithShardAwarePort())
                      .withSslContext(context)
                      .withConfigLoader(configLoader)
                      .build()) {
        healthCheck(session);

        // Perform a node restart to force all connections to be re-established
        initialResumptions = resumptions.get();
        initialHellos = serverHellos.get();
        initialPsks = pskUses.get();
        CCM_RULE.getCcmBridge().stop();
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
        CCM_RULE.getCcmBridge().start();
        healthCheck(session);
        reconnectionResumptions = resumptions.get() - initialResumptions;
        reconnectionHellos = serverHellos.get() - initialHellos;
        reconnectionPsks = pskUses.get() - initialPsks;
      }
    } finally {
      handler.flush();
    }

    Assert.assertEquals(
        "Each connection should have negotiated TLSv1.3.",
        serverHellos.get(),
        negotiatedTls13.get());
    Assert.assertTrue("Client should have received some tickets.", ticketsReceived.get() > 0);
    Assert.assertTrue(
        String.format(
            "Each reconnection but first should be a resumption. Meanwhile found %s ServerHellos and %s resumptions.",
            reconnectionHellos, reconnectionResumptions),
        reconnectionResumptions >= reconnectionHellos - 1);
    Assert.assertEquals(
        "PSK should have been used for each resumption on reconnection.",
        reconnectionResumptions,
        reconnectionPsks);
  }

  private void healthCheck(CqlSession session) {
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                if (session
                    instanceof com.datastax.oss.driver.internal.core.session.DefaultSession) {
                  com.datastax.oss.driver.internal.core.session.DefaultSession defaultSession =
                      (com.datastax.oss.driver.internal.core.session.DefaultSession) session;

                  Map<Node, com.datastax.oss.driver.internal.core.pool.ChannelPool> pools =
                      defaultSession.getPools();

                  // Check that all nodes have pools and channels are initialized
                  for (Map.Entry<Node, com.datastax.oss.driver.internal.core.pool.ChannelPool>
                      entry : pools.entrySet()) {
                    com.datastax.oss.driver.internal.core.pool.ChannelPool pool = entry.getValue();
                    // Assuming 1 connection per shard.
                    if (pool == null || pool.size() != smpValue) {
                      return false; // Pool not ready or not enough channels available
                    }

                    if (pool.getAvailableIds() == 0) {
                      return false;
                    }
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

  private SSLContext createSslContext(String protocol) throws Exception {
    SSLContext context = SSLContext.getInstance(protocol);

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    try (InputStream tsf =
        Files.newInputStream(
            Paths.get(CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath()))) {
      KeyStore ts = KeyStore.getInstance("JKS");
      char[] password = CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD.toCharArray();
      ts.load(tsf, password);
      tmf.init(ts);
    }

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    try (InputStream ksf =
        Files.newInputStream(Paths.get(CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath()))) {
      KeyStore ks = KeyStore.getInstance("JKS");
      char[] password = CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray();
      ks.load(ksf, password);
      kmf.init(ks, password);
    }

    context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return context;
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
