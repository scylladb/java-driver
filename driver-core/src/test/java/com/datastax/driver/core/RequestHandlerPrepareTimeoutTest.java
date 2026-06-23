/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.objenesis.ObjenesisStd;
import org.testng.annotations.Test;

public class RequestHandlerPrepareTimeoutTest {

  @Test(groups = "unit")
  public void should_capture_prepare_timeout_diagnostics_before_releasing_connection()
      throws Exception {
    RequestHandler handler = allocateInstance(RequestHandler.class);
    setField(handler, "id", "test");
    setField(
        handler,
        "queryPlan",
        new RequestHandler.QueryPlan(Collections.<Host>emptyList().iterator()));
    Set<Object> runningExecutions = new CopyOnWriteArraySet<Object>();
    runningExecutions.add(new Object());
    setField(handler, "runningExecutions", runningExecutions);
    setField(handler, "isDone", new AtomicBoolean(false));

    RequestHandler.SpeculativeExecution execution =
        handler.new SpeculativeExecution(new Requests.Query("SELECT v FROM test WHERE k = ?"), 2);

    Method prepareAndRetry =
        RequestHandler.SpeculativeExecution.class.getDeclaredMethod(
            "prepareAndRetry", String.class);
    prepareAndRetry.setAccessible(true);
    Connection.ResponseCallback callback =
        (Connection.ResponseCallback)
            prepareAndRetry.invoke(execution, "SELECT v FROM test WHERE k = ?");

    EndPoint endPoint = EndPoints.forAddress("127.0.0.1", 9042);
    RecordingConnection connection = allocateInstance(RecordingConnection.class);
    connection.calls = new ArrayList<String>();
    setField(connection, "endPoint", endPoint);

    connection.timeoutException = new OperationTimedOutException(endPoint);

    setField(
        execution,
        "queryStateRef",
        new AtomicReference<RequestHandler.QueryState>(
            RequestHandler.QueryState.INITIAL.startNext()));

    callback.onTimeout(connection, 123L, 0);

    assertThat(connection.calls).containsExactly("newTimeoutException", "release");
    assertThat(connection.message).isEqualTo("Timed out waiting for response to PREPARE message");
    assertThat(connection.configuredTimeoutMs).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(connection.elapsedTimeoutNanos).isEqualTo(123L);
    assertThat(connection.retryCount).isEqualTo(0);
    assertThat(connection.speculativeExecutionIndex).isEqualTo(2);
  }

  @SuppressWarnings("unchecked")
  private static <T> T allocateInstance(Class<T> clazz) throws Exception {
    return (T) new ObjenesisStd().newInstance(clazz);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Class<?> current = target.getClass();
    while (current != null) {
      try {
        Field field = current.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }

  private static class RecordingConnection extends Connection {

    private List<String> calls;
    private OperationTimedOutException timeoutException;
    private String message;
    private long configuredTimeoutMs;
    private long elapsedTimeoutNanos;
    private int retryCount;
    private int speculativeExecutionIndex;

    private RecordingConnection() {
      super(null, null, null);
    }

    @Override
    OperationTimedOutException newTimeoutException(
        String message,
        long configuredTimeoutMs,
        long elapsedTimeoutNanos,
        int retryCount,
        int speculativeExecutionIndex) {
      calls.add("newTimeoutException");
      this.message = message;
      this.configuredTimeoutMs = configuredTimeoutMs;
      this.elapsedTimeoutNanos = elapsedTimeoutNanos;
      this.retryCount = retryCount;
      this.speculativeExecutionIndex = speculativeExecutionIndex;
      return timeoutException;
    }

    @Override
    void release() {
      calls.add("release");
    }
  }
}
