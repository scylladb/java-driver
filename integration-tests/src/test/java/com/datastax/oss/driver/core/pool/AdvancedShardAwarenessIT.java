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
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.ScyllaOnly;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@ScyllaOnly(description = "Advanced shard awareness relies on ScyllaDB's shard aware port")
@RunWith(DataProviderRunner.class)
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
  private final Pattern reconnectionPattern =
      Pattern.compile(".*Scheduling next reconnection in.*");
  Set<Pattern> forbiddenOccurences = ImmutableSet.of(shardMismatchPattern, reconnectionPattern);

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
    Map<Pattern, Integer> expectedOccurences =
        ImmutableMap.of(
            Pattern.compile(".*\\.2:19042.*Reconnection attempt complete, 6/6 channels.*"), 1,
            Pattern.compile(".*\\.1:19042.*Reconnection attempt complete, 6/6 channels.*"), 1,
            Pattern.compile(".*Reconnection attempt complete.*"), 2,
            Pattern.compile(".*\\.1:19042.*New channel added \\[.*"), 5,
            Pattern.compile(".*\\.2:19042.*New channel added \\[.*"), 5,
            Pattern.compile(".*\\.1:19042\\] Trying to create 5 missing channels.*"), 1,
            Pattern.compile(".*\\.2:19042\\] Trying to create 5 missing channels.*"), 1);
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS, reuseAddress)
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW, 10000)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH, 60000)
            // Due to rounding up the connections per shard this will result in 6 connections per
            // node
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
            .build();
    try (Session session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CCM_RULE.getCcmBridge().getNodeIpAddress(1), 19042))
            .withConfigLoader(loader)
            .build()) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      expectedOccurences.forEach(
          (pattern, times) -> assertMatchesExactly(pattern, times, appender.list));
      forbiddenOccurences.forEach(pattern -> assertNoLogMatches(pattern, appender.list));
    }
  }

  @Test
  public void should_see_mismatched_shard() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW, 10000)
            .withInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH, 60000)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 64)
            .build();
    try (Session session =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(CCM_RULE.getCcmBridge().getNodeIpAddress(1), 9042))
            .withConfigLoader(loader)
            .build()) {
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      assertMatchesAtLeast(shardMismatchPattern, 5, appender.list);
    }
  }

  // There is no need to run this as a test, but it serves as a comparison
  @SuppressWarnings("unused")
  public void should_struggle_to_fill_pools() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, false)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 64)
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
      Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
      assertNoLogMatches(shardMismatchPattern, appender.list);
      assertMatchesAtLeast(reconnectionPattern, 8, appender.list);
    }
  }

  @Test
  public void should_not_struggle_to_fill_pools() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true)
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 66)
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(10))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofMillis(20))
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
      Uninterruptibles.sleepUninterruptibly(8, TimeUnit.SECONDS);
      int tolerance = 2; // Sometimes socket ends up already in use
      Map<Pattern, Integer> expectedOccurences =
          ImmutableMap.of(
              Pattern.compile(".*\\.2:19042.*Reconnection attempt complete, 66/66 channels.*"),
                  1 * sessions,
              Pattern.compile(".*\\.1:19042.*Reconnection attempt complete, 66/66 channels.*"),
                  1 * sessions,
              Pattern.compile(".*Reconnection attempt complete.*"), 2 * sessions,
              Pattern.compile(".*.1:19042.*New channel added \\[.*"), 65 * sessions - tolerance,
              Pattern.compile(".*.2:19042.*New channel added \\[.*"), 65 * sessions - tolerance,
              Pattern.compile(".*.1:19042\\] Trying to create 65 missing channels.*"), 1 * sessions,
              Pattern.compile(".*.2:19042\\] Trying to create 65 missing channels.*"),
                  1 * sessions);
      expectedOccurences.forEach(
          (pattern, times) -> assertMatchesAtLeast(pattern, times, appender.list));
      assertNoLogMatches(shardMismatchPattern, appender.list);
      assertMatchesAtMost(reconnectionPattern, tolerance, appender.list);
    }
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
