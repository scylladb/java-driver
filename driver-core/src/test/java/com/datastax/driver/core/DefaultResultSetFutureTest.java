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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link DefaultResultSetFuture}.
 *
 * <p>These tests verify that the future properly completes for various schema change response
 * types. Issue #365 identified that an unknown schema change type would leave the future incomplete
 * forever.
 *
 * @jira_ticket 365
 */
public class DefaultResultSetFutureTest {

  /**
   * Tests that {@link DefaultResultSetFuture#onSet} properly completes the future when a schema
   * change response is received, even for edge cases like unknown change types.
   *
   * <p>This test verifies the fix for issue #365 where the future could hang forever. The test
   * creates a synthetic enum value to simulate an unknown schema change type from a future protocol
   * version.
   *
   * <p><b>Note:</b> Due to Java's enum switch implementation, testing the exact "before" state
   * (future not completing) is difficult. This test verifies the future completes correctly with
   * the fix in place.
   *
   * @jira_ticket 365
   */
  @Test(groups = "unit")
  public void should_complete_future_for_schema_change_response() throws Exception {
    // Create minimal stub objects using reflection
    Object[] testObjects = createTestObjects();
    SessionManager session = (SessionManager) testObjects[0];
    Connection connection = (Connection) testObjects[1];

    // Create the future
    DefaultResultSetFuture future = new DefaultResultSetFuture(session, ProtocolVersion.V4, null);

    // Create a SchemaChange response with a synthetic "unknown" change type
    // This simulates a scenario where a future protocol version adds a new change type
    Responses.Result.SchemaChange schemaChange = createSchemaChangeWithUnknownType();

    // Call onSet - the fix ensures the future always completes
    future.onSet(connection, schemaChange, null, null, 0);

    // Verify the future completes within a reasonable timeout
    try {
      future.getUninterruptibly(1, TimeUnit.SECONDS);
      assertTrue(future.isDone(), "Future should be done");
    } catch (TimeoutException e) {
      fail("Future did not complete within timeout: " + e);
    } catch (Exception e) {
      // Exceptions from incomplete stubs are OK - the key is that the future completed
      assertTrue(future.isDone(), "Future should be done even with exception: " + e.getMessage());
    }
  }

  /**
   * Creates minimal test objects needed for testing. Uses Unsafe to create instances without
   * invoking constructors.
   */
  private Object[] createTestObjects() throws Exception {
    Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
    Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
    unsafeField.setAccessible(true);
    Object unsafe = unsafeField.get(null);
    java.lang.reflect.Method allocateInstance =
        unsafeClass.getMethod("allocateInstance", Class.class);

    // Create QueryOptions with metadata enabled
    QueryOptions queryOptions = new QueryOptions();
    queryOptions.setMetadataEnabled(true);

    // Create Configuration
    Configuration configuration =
        (Configuration) allocateInstance.invoke(unsafe, Configuration.class);
    setField(configuration, "queryOptions", queryOptions);

    // Create Cluster.Manager
    Cluster.Manager manager =
        (Cluster.Manager) allocateInstance.invoke(unsafe, Cluster.Manager.class);
    setField(manager, "configuration", configuration);

    // Create Cluster
    Cluster cluster = (Cluster) allocateInstance.invoke(unsafe, Cluster.class);
    setField(cluster, "manager", manager);

    // Create SessionManager
    SessionManager session = (SessionManager) allocateInstance.invoke(unsafe, SessionManager.class);
    setField(session, "cluster", cluster);

    // Create Connection with ProtocolFeatureStore
    Connection connection = (Connection) allocateInstance.invoke(unsafe, Connection.class);
    setField(connection, "protocolFeatureStore", new ProtocolFeatureStore(null, null, null, false));

    return new Object[] {session, connection};
  }

  /** Helper to set a field value using reflection. */
  private void setField(Object obj, String fieldName, Object value) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(obj, value);
  }

  /**
   * Creates a SchemaChange response with a synthetic enum value that won't match CREATED, UPDATED,
   * or DROPPED.
   */
  private Responses.Result.SchemaChange createSchemaChangeWithUnknownType() throws Exception {
    Responses.Result.SchemaChange.Change unknownChange = createSyntheticEnumValue();

    Constructor<Responses.Result.SchemaChange> constructor =
        Responses.Result.SchemaChange.class.getDeclaredConstructor(
            Responses.Result.SchemaChange.Change.class,
            SchemaElement.class,
            String.class,
            String.class,
            java.util.List.class);
    constructor.setAccessible(true);

    return constructor.newInstance(
        unknownChange, SchemaElement.KEYSPACE, "test_keyspace", "", Collections.emptyList());
  }

  /** Creates a synthetic enum value for testing unknown schema change types. */
  private Responses.Result.SchemaChange.Change createSyntheticEnumValue() throws Exception {
    Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
    Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
    unsafeField.setAccessible(true);
    Object unsafe = unsafeField.get(null);

    java.lang.reflect.Method allocateInstance =
        unsafeClass.getMethod("allocateInstance", Class.class);
    java.lang.reflect.Method objectFieldOffset =
        unsafeClass.getMethod("objectFieldOffset", Field.class);
    java.lang.reflect.Method putObject =
        unsafeClass.getMethod("putObject", Object.class, long.class, Object.class);
    java.lang.reflect.Method putInt =
        unsafeClass.getMethod("putInt", Object.class, long.class, int.class);

    // Create enum instance without constructor
    Responses.Result.SchemaChange.Change syntheticValue =
        (Responses.Result.SchemaChange.Change)
            allocateInstance.invoke(unsafe, Responses.Result.SchemaChange.Change.class);

    // Set name and ordinal fields
    Field nameField = Enum.class.getDeclaredField("name");
    Field ordinalField = Enum.class.getDeclaredField("ordinal");

    long nameOffset = (Long) objectFieldOffset.invoke(unsafe, nameField);
    long ordinalOffset = (Long) objectFieldOffset.invoke(unsafe, ordinalField);

    putObject.invoke(unsafe, syntheticValue, nameOffset, "UNKNOWN_FOR_TESTING");
    putInt.invoke(unsafe, syntheticValue, ordinalOffset, 99);

    return syntheticValue;
  }
}
