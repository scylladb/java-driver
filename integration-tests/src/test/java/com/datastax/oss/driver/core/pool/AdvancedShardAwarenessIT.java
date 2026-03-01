package com.datastax.oss.driver.core.pool;

import static junit.framework.TestCase.fail;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ScyllaOnly;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@ScyllaOnly(description = "Advanced shard awareness relies on ScyllaDB's shard aware port")
@RunWith(DataProviderRunner.class)
@Category(IsolatedTests.class)
public class AdvancedShardAwarenessIT {

  @ClassRule
  public static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder().withNodes(2).withJvmArgs("--smp=3").build();

  public static ch.qos.logback.classic.Logger channelPoolLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ChannelPool.class);
  public static ch.qos.logback.classic.Logger reconnectionLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Reconnection.class);
  ListAppender<ILoggingEvent> appender;
  Level originalLevelChannelPool;
  Level originalLevelReconnection;
  private final Pattern shardMismatchPattern =
      Pattern.compile(".*r configuration of shard aware port.*");
  private final Pattern generalReconnectionPattern =
      Pattern.compile(".*Scheduling next reconnection in.*");

  @DataProvider
  public static Object[][] reuseAddressOption() {
    return new Object[][] {{true}, {false}};
  }

  @Before
  public void startCapturingLogs() {
    originalLevelChannelPool = channelPoolLogger.getLevel();
    originalLevelReconnection = reconnectionLogger.getLevel();
    channelPoolLogger.setLevel(Level.DEBUG);
    reconnectionLogger.setLevel(Level.DEBUG);
    appender = new ListAppender<>();
    appender.setContext(
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).getLoggerContext());
    channelPoolLogger.addAppender(appender);
    reconnectionLogger.addAppender(appender);
    appender.list.clear();
    appender.start();
  }

  @After
  public void stopCapturingLogs() {
    appender.stop();
    appender.list.clear();
    channelPoolLogger.setLevel(originalLevelChannelPool);
    reconnectionLogger.setLevel(originalLevelReconnection);
    channelPoolLogger.detachAppender(appender);
    reconnectionLogger.detachAppender(appender);
  }

  @Test
  @UseDataProvider("reuseAddressOption")
  public void should_initialize_all_channels(boolean reuseAddress) {
    int expectedChannelsPerNode = 6; // Divisible by smp
    String node1 = CCM_RULE.getCcmBridge().getNodeIpAddress(1);
    String node2 = CCM_RULE.getCcmBridge().getNodeIpAddress(2);
    Pattern reconnectionPattern1 =
        Pattern.compile(".*" + Pattern.quote(node1) + ".*Scheduling next reconnection in.*");
    Pattern reconnectionPattern2 =
        Pattern.compile(".*" + Pattern.quote(node2) + ".*Scheduling next reconnection in.*");
    Set<Pattern> forbiddenOccurences =
        ImmutableSet.of(shardMismatchPattern, reconnectionPattern1, reconnectionPattern2);
    Map<Pattern, Integer> expectedOccurences =
        ImmutableMap.of(
            Pattern.compile(
                    ".*"
                        + Pattern.quote(node1)
                        + ":19042.*Reconnection attempt complete, 6/6 channels.*"),
                1,
            Pattern.compile(
                    ".*"
                        + Pattern.quote(node2)
                        + ":19042.*Reconnection attempt complete, 6/6 channels.*"),
                1);
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS, reuseAddress)
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW, 10000)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH, 60000)
            // Due to rounding up the connections per shard this will result in 6 connections per
            // node
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, expectedChannelsPerNode)
            .build();
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CCM_RULE.getCcmBridge().getNodeIpAddress(1), 19042))
            .withConfigLoader(loader)
            .build()) {
      List<CqlSession> allSessions = Collections.singletonList(session);
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> areAllPoolsFullyInitialized(allSessions, expectedChannelsPerNode));
      List<ILoggingEvent> logsCopy = ImmutableList.copyOf(appender.list);
      expectedOccurences.forEach(
          (pattern, times) -> assertMatchesExactly(pattern, times, logsCopy));
      forbiddenOccurences.forEach(pattern -> assertNoLogMatches(pattern, logsCopy));
    }
  }

  @Test
  public void should_see_mismatched_shard() {
    int expectedChannelsPerNode = 66; // Divisible by smp
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW, 10000)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH, 60000)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 66)
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(30))
            .build();
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CCM_RULE.getCcmBridge().getNodeIpAddress(1), 9042))
            .withConfigLoader(loader)
            .build()) {
      List<CqlSession> allSessions = Collections.singletonList(session);
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> areAllPoolsFullyInitialized(allSessions, expectedChannelsPerNode));
      List<ILoggingEvent> logsCopy = ImmutableList.copyOf(appender.list);
      assertMatchesAtLeast(shardMismatchPattern, 5, logsCopy);
    }
  }

  // There is no need to run this as a test, but it serves as a comparison
  @SuppressWarnings("unused")
  public void should_struggle_to_fill_pools() {
    int expectedChannelsPerNode = 66; // Divisible by smp
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, false)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 66)
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(200))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofMillis(4000))
            .build();
    CqlSessionBuilder builder =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CCM_RULE.getCcmBridge().getNodeIpAddress(1), 9042))
            .withConfigLoader(loader);
    CompletionStage<CqlSession> stage1 = builder.buildAsync();
    CompletionStage<CqlSession> stage2 = builder.buildAsync();
    CompletionStage<CqlSession> stage3 = builder.buildAsync();
    CompletionStage<CqlSession> stage4 = builder.buildAsync();
    try (CqlSession session1 = CompletableFutures.getUninterruptibly(stage1);
        CqlSession session2 = CompletableFutures.getUninterruptibly(stage2);
        CqlSession session3 = CompletableFutures.getUninterruptibly(stage3);
        CqlSession session4 = CompletableFutures.getUninterruptibly(stage4); ) {
      List<CqlSession> allSessions = Arrays.asList(session1, session2, session3, session4);
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> areAllPoolsFullyInitialized(allSessions, expectedChannelsPerNode));
      List<ILoggingEvent> logsCopy = ImmutableList.copyOf(appender.list);
      assertNoLogMatches(shardMismatchPattern, logsCopy);
      assertMatchesAtLeast(generalReconnectionPattern, 8, logsCopy);
    }
  }

  @Test
  public void should_not_struggle_to_fill_pools() {
    int expectedChannelsPerNode = 66;
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, expectedChannelsPerNode)
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(10))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofMillis(20))
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(30))
            .build();
    CqlSessionBuilder builder =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CCM_RULE.getCcmBridge().getNodeIpAddress(1), 19042))
            .withConfigLoader(loader);
    CompletionStage<CqlSession> stage1 = builder.buildAsync();
    CompletionStage<CqlSession> stage2 = builder.buildAsync();
    CompletionStage<CqlSession> stage3 = builder.buildAsync();
    CompletionStage<CqlSession> stage4 = builder.buildAsync();
    int sessions = 4;
    try (CqlSession session1 = CompletableFutures.getUninterruptibly(stage1);
        CqlSession session2 = CompletableFutures.getUninterruptibly(stage2);
        CqlSession session3 = CompletableFutures.getUninterruptibly(stage3);
        CqlSession session4 = CompletableFutures.getUninterruptibly(stage4); ) {
      List<CqlSession> allSessions = Arrays.asList(session1, session2, session3, session4);
      Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .until(() -> areAllPoolsFullyInitialized(allSessions, expectedChannelsPerNode));
      int tolerance = 2; // Sometimes socket ends up already in use
      String node1 = CCM_RULE.getCcmBridge().getNodeIpAddress(1);
      String node2 = CCM_RULE.getCcmBridge().getNodeIpAddress(2);
      Pattern reconnectionPattern1 =
          Pattern.compile(".*" + Pattern.quote(node1) + ".*Scheduling next reconnection in.*");
      Pattern reconnectionPattern2 =
          Pattern.compile(".*" + Pattern.quote(node2) + ".*Scheduling next reconnection in.*");
      Map<Pattern, Integer> expectedOccurences =
          ImmutableMap.of(
              Pattern.compile(
                      ".*"
                          + Pattern.quote(node1)
                          + ":19042.*Reconnection attempt complete, 66/66 channels.*"),
                  1 * sessions,
              Pattern.compile(
                      ".*"
                          + Pattern.quote(node2)
                          + ":19042.*Reconnection attempt complete, 66/66 channels.*"),
                  1 * sessions);
      List<ILoggingEvent> logsCopy = ImmutableList.copyOf(appender.list);
      expectedOccurences.forEach(
          (pattern, times) -> assertMatchesAtLeast(pattern, times, logsCopy));
      assertNoLogMatches(shardMismatchPattern, logsCopy);
      assertMatchesAtMost(reconnectionPattern1, tolerance, logsCopy);
      assertMatchesAtMost(reconnectionPattern2, tolerance, logsCopy);
    }
  }

  private boolean areAllPoolsFullyInitialized(
      List<CqlSession> sessions, int expectedChannelsPerNode) {
    for (CqlSession session : sessions) {
      DefaultSession defaultSession = (DefaultSession) session;
      Map<Node, ChannelPool> pools = defaultSession.getPools();
      if (pools == null || pools.isEmpty()) {
        return false;
      }

      for (ChannelPool pool : pools.values()) {
        if (pool == null) {
          return false;
        }
        if (pool.size() < expectedChannelsPerNode) {
          return false;
        }
      }
    }
    return true;
  }

  private void assertNoLogMatches(Pattern pattern, List<ILoggingEvent> logs) {
    for (ILoggingEvent log : logs) {
      if (pattern.matcher(log.getFormattedMessage()).matches()) {
        fail(
            "Logs should not contain pattern ["
                + pattern.toString()
                + "] but found in ["
                + log.getFormattedMessage()
                + "]");
      }
    }
  }

  private void assertMatchesExactly(Pattern pattern, Integer times, List<ILoggingEvent> logs) {
    int occurences = 0;
    for (ILoggingEvent log : logs) {
      if (pattern.matcher(log.getFormattedMessage()).matches()) {
        occurences++;
      }
    }
    if (occurences != times) {
      fail(
          "Expected to find pattern exactly "
              + times
              + " times but found it "
              + occurences
              + " times. Pattern: ["
              + pattern.toString()
              + "]");
    }
  }

  private void assertMatchesAtLeast(Pattern pattern, Integer times, List<ILoggingEvent> logs) {
    int occurences = 0;
    for (ILoggingEvent log : logs) {
      if (pattern.matcher(log.getFormattedMessage()).matches()) {
        occurences++;
        if (occurences >= times) {
          return;
        }
      }
    }
    fail(
        "Expected to find pattern at least "
            + times
            + " times but found only "
            + occurences
            + " times. Pattern: ["
            + pattern.toString()
            + "]");
  }

  private void assertMatchesAtMost(Pattern pattern, Integer times, List<ILoggingEvent> logs) {
    int occurences = 0;
    for (ILoggingEvent log : logs) {
      if (pattern.matcher(log.getFormattedMessage()).matches()) {
        occurences++;
        if (occurences > times) {
          fail(
              "Expected to find pattern at most "
                  + times
                  + " times but found it "
                  + occurences
                  + " times. Pattern: ["
                  + pattern.toString()
                  + "]");
        }
      }
    }
  }
}
