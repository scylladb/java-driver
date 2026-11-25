package com.datastax.oss.driver.api.testinfra.session;

import com.datastax.oss.driver.internal.core.session.SessionRegistry;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class SessionTracker {
  static final TestSessionRegistry sessionRegistry = new TestSessionRegistry();

  private static final Set<String> runningTests = new ConcurrentSkipListSet<>();

  public static void testStarted(String className, String methodName) {
    runningTests.add(String.format("%s.%s", className, methodName));
  }

  public static void testEnded(String className, String methodName) {
    runningTests.remove(String.format("%s.%s", className, methodName));
    if (runningTests.isEmpty()) {
      List<TestSessionRegistry.SessionRecord> activeSessions =
          sessionRegistry.getActiveSessionsAndForget();
      if (!activeSessions.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "There are active sessions, created in following tests: %s",
                activeSessions.stream()
                    .flatMap(s -> s.sourceTests.stream())
                    .collect(Collectors.toList())));
      }
    }
  }

  private static class TestSessionRegistry extends SessionRegistry {
    protected TestSessionRegistry() {
      super();
    }

    public static class SessionRecord {
      final WeakReference<Object> session;
      final Set<String> sourceTests;

      SessionRecord(WeakReference<Object> session, Set<String> sourceTests) {
        this.session = session;
        this.sourceTests = sourceTests;
      }
    }

    private static final List<SessionRecord> sessions = new CopyOnWriteArrayList<>();

    @Override
    public void registerSession(Object session) {
      sessions.add(
          new SessionRecord(
              new WeakReference<>(session), runningTests.stream().collect(Collectors.toSet())));
    }

    @Override
    public void closeSession(Object session) {
      sessions.removeIf(s -> s.session == session);
    }

    public List<SessionRecord> getActiveSessionsAndForget() {
      // Purge known sessions
      sessions.removeIf(ref -> ref.session.get() == null);
      return sessions.stream()
          .filter(ref -> ref.session.get() == null)
          .collect(Collectors.toList());
    }
  }
}
