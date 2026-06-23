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

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Queue;
import org.objenesis.ObjenesisStd;
import org.testng.annotations.Test;

public class HostConnectionPoolTimeoutDiagnosticsTest {

  @Test(groups = "unit")
  public void should_report_pending_borrow_queue_size_per_shard() throws Exception {
    HostConnectionPool pool = allocateInstance(HostConnectionPool.class);
    Queue<Object> pendingBorrows = new ArrayDeque<Object>();
    for (int i = 0; i < 6; i++) {
      pendingBorrows.add(new Object());
    }
    setField(pool, "pendingBorrows", new Queue[] {pendingBorrows});

    assertEquals(pool.pendingBorrowCountForShard(0), 6);
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
}
