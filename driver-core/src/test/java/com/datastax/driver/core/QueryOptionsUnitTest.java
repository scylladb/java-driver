/*
 * Copyright ScyllaDB, Inc.
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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.QueryOptions.CQL4SkipMetadataResolveMethod;
import com.datastax.driver.core.QueryOptions.RequestRoutingMethod;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.google.common.util.concurrent.Futures;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class QueryOptionsUnitTest {

  @Test(groups = "unit")
  public void should_be_equal_to_itself_and_to_default_instance() {
    QueryOptions options = new QueryOptions();
    assertThat(options).isEqualTo(options);
    assertThat(options).isEqualTo(new QueryOptions());
    assertThat(options.hashCode()).isEqualTo(new QueryOptions().hashCode());
  }

  @Test(groups = "unit")
  public void should_not_be_equal_to_null_or_other_type() {
    QueryOptions options = new QueryOptions();
    assertThat(options.equals(null)).isFalse();
    assertThat(options.equals("QueryOptions")).isFalse();
  }

  @Test(groups = "unit")
  public void should_be_equal_when_all_compared_fields_match() {
    assertThat(customized()).isEqualTo(customized());
    assertThat(customized().hashCode()).isEqualTo(customized().hashCode());
    assertThat(customized()).isNotEqualTo(new QueryOptions());
  }

  @Test(groups = "unit")
  public void should_not_be_equal_when_any_compared_field_differs() {
    assertDiffers(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM));
    assertDiffers(new QueryOptions().setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL));
    assertDiffers(new QueryOptions().setFetchSize(42));
    assertDiffers(new QueryOptions().setDefaultIdempotence(true));
    assertDiffers(new QueryOptions().setMetadataEnabled(false));
    assertDiffers(new QueryOptions().setMaxPendingRefreshNodeListRequests(1));
    assertDiffers(new QueryOptions().setMaxPendingRefreshNodeRequests(1));
    assertDiffers(new QueryOptions().setMaxPendingRefreshSchemaRequests(1));
    assertDiffers(new QueryOptions().setRefreshNodeListIntervalMillis(1));
    assertDiffers(new QueryOptions().setRefreshNodeIntervalMillis(1));
    assertDiffers(new QueryOptions().setRefreshSchemaIntervalMillis(1));
    assertDiffers(new QueryOptions().setReprepareOnUp(false));
    assertDiffers(new QueryOptions().setPrepareOnAllHosts(false));
    assertDiffers(
        new QueryOptions().setLoadBalancingLwtRequestRoutingMethod(RequestRoutingMethod.REGULAR));
    assertDiffers(new QueryOptions().setSchemaQueriesPaged(false));
  }

  @Test(groups = "unit")
  public void should_ignore_three_scylla_options_in_equals_and_hashCode() {
    // Pins current behaviour (#1177): equals/hashCode omit three Scylla-added fields.
    assertIgnored(
        new QueryOptions()
            .setSkipCQL4MetadataResolveMethod(CQL4SkipMetadataResolveMethod.DISABLED));
    assertIgnored(new QueryOptions().setAddOriginalContactsToReconnectionPlan(true));
    assertIgnored(new QueryOptions().setConsiderZeroTokenNodesValidPeers(true));
  }

  @Test(groups = "unit")
  public void should_throw_npe_in_equals_when_serial_consistency_is_null() {
    // Pins current behaviour (#1188): equals dereferences serialConsistency, which may be null.
    QueryOptions withNull = new QueryOptions().setSerialConsistencyLevel(null);
    assertThat(new QueryOptions().equals(withNull)).isFalse();
    try {
      withNull.equals(new QueryOptions());
      fail("Expected NullPointerException");
    } catch (NullPointerException expected) {
    }
  }

  @Test(groups = "unit")
  public void should_set_and_get_scylla_specific_options() {
    QueryOptions options = new QueryOptions();
    assertThat(options.shouldAddOriginalContactsToReconnectionPlan()).isFalse();
    assertThat(options.shouldConsiderZeroTokenNodesValidPeers()).isFalse();
    assertThat(options.getLoadBalancingLwtRequestRoutingMethod())
        .isEqualTo(RequestRoutingMethod.PRESERVE_REPLICA_ORDER);

    assertThat(options.setAddOriginalContactsToReconnectionPlan(true)).isSameAs(options);
    assertThat(options.setConsiderZeroTokenNodesValidPeers(true)).isSameAs(options);
    assertThat(options.setLoadBalancingLwtRequestRoutingMethod(RequestRoutingMethod.REGULAR))
        .isSameAs(options);

    assertThat(options.shouldAddOriginalContactsToReconnectionPlan()).isTrue();
    assertThat(options.shouldConsiderZeroTokenNodesValidPeers()).isTrue();
    assertThat(options.getLoadBalancingLwtRequestRoutingMethod())
        .isEqualTo(RequestRoutingMethod.REGULAR);
  }

  @Test(groups = "unit")
  public void should_return_configured_values() {
    QueryOptions options =
        customized().setSkipCQL4MetadataResolveMethod(CQL4SkipMetadataResolveMethod.ENABLED);
    assertThat(options.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);
    assertThat(options.getSerialConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);
    assertThat(options.getFetchSize()).isEqualTo(10);
    assertThat(options.getDefaultIdempotence()).isTrue();
    assertThat(options.isMetadataEnabled()).isFalse();
    assertThat(options.getMaxPendingRefreshNodeListRequests()).isEqualTo(2);
    assertThat(options.getMaxPendingRefreshNodeRequests()).isEqualTo(3);
    assertThat(options.getMaxPendingRefreshSchemaRequests()).isEqualTo(4);
    assertThat(options.getRefreshNodeListIntervalMillis()).isEqualTo(5);
    assertThat(options.getRefreshNodeIntervalMillis()).isEqualTo(6);
    assertThat(options.getRefreshSchemaIntervalMillis()).isEqualTo(7);
    assertThat(options.isReprepareOnUp()).isFalse();
    assertThat(options.isPrepareOnAllHosts()).isFalse();
    assertThat(options.isSchemaQueriesPaged()).isFalse();
    assertThat(options.getSkipCQL4MetadataResolveMethod())
        .isEqualTo(CQL4SkipMetadataResolveMethod.ENABLED);
  }

  @Test(groups = "unit")
  public void should_refresh_node_list_then_schema_when_metadata_is_re_enabled() {
    Cluster.Manager manager = mock(Cluster.Manager.class);
    when(manager.submitNodeListRefresh()).thenReturn(Futures.<Void>immediateFuture(null));
    QueryOptions options = new QueryOptions().setMetadataEnabled(false);
    options.register(manager);

    options.setMetadataEnabled(true);

    InOrder inOrder = inOrder(manager);
    inOrder.verify(manager).submitNodeListRefresh();
    inOrder.verify(manager).submitSchemaRefresh(null, null, null, null);
  }

  @Test(groups = "unit")
  public void should_not_refresh_when_metadata_was_already_enabled() {
    Cluster.Manager manager = mock(Cluster.Manager.class);
    QueryOptions options = new QueryOptions();
    options.register(manager);

    options.setMetadataEnabled(true);
    options.setMetadataEnabled(false);

    verify(manager, never()).submitNodeListRefresh();
  }

  @Test(groups = "unit")
  public void should_not_refresh_when_metadata_stays_disabled() {
    Cluster.Manager manager = mock(Cluster.Manager.class);
    QueryOptions options = new QueryOptions().setMetadataEnabled(false);
    options.register(manager);

    options.setMetadataEnabled(false);

    verify(manager, never()).submitNodeListRefresh();
  }

  @Test(groups = "unit")
  public void should_not_refresh_when_metadata_is_re_enabled_before_registration() {
    QueryOptions options = new QueryOptions().setMetadataEnabled(false);

    options.setMetadataEnabled(true);

    assertThat(options.isMetadataEnabled()).isTrue();
  }

  @Test(groups = "unit")
  public void should_track_whether_consistency_was_set() {
    QueryOptions options = new QueryOptions();
    assertThat(options.isConsistencySet()).isFalse();
    options.setConsistencyLevel(ConsistencyLevel.ONE);
    assertThat(options.isConsistencySet()).isTrue();
  }

  @Test(groups = "unit")
  public void should_reject_non_positive_fetch_size() {
    for (int fetchSize : new int[] {0, -1}) {
      try {
        new QueryOptions().setFetchSize(fetchSize);
        fail("Expected IllegalArgumentException for fetchSize " + fetchSize);
      } catch (IllegalArgumentException e) {
        assertThat(e.getMessage()).isEqualTo("Invalid fetchSize, should be > 0, got " + fetchSize);
      }
    }
  }

  @Test(groups = "unit")
  public void should_reject_paging_fetch_size_with_protocol_v1() {
    QueryOptions options = withProtocolVersion(ProtocolVersion.V1);
    try {
      options.setFetchSize(100);
      fail("Expected UnsupportedFeatureException");
    } catch (UnsupportedFeatureException e) {
      assertThat(e.getMessage()).contains("Paging is not supported");
    }
    assertThat(options.getFetchSize()).isEqualTo(QueryOptions.DEFAULT_FETCH_SIZE);
  }

  @Test(groups = "unit")
  public void should_allow_disabling_paging_with_protocol_v1() {
    QueryOptions options = withProtocolVersion(ProtocolVersion.V1);
    options.setFetchSize(Integer.MAX_VALUE);
    assertThat(options.getFetchSize()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test(groups = "unit")
  public void should_allow_paging_fetch_size_with_protocol_v2() {
    QueryOptions options = withProtocolVersion(ProtocolVersion.V2);
    options.setFetchSize(100);
    assertThat(options.getFetchSize()).isEqualTo(100);
  }

  private static QueryOptions withProtocolVersion(ProtocolVersion version) {
    Cluster.Manager manager = mock(Cluster.Manager.class);
    when(manager.protocolVersion()).thenReturn(version);
    QueryOptions options = new QueryOptions();
    options.register(manager);
    return options;
  }

  private static QueryOptions customized() {
    return new QueryOptions()
        .setConsistencyLevel(ConsistencyLevel.ALL)
        .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
        .setFetchSize(10)
        .setDefaultIdempotence(true)
        .setMetadataEnabled(false)
        .setMaxPendingRefreshNodeListRequests(2)
        .setMaxPendingRefreshNodeRequests(3)
        .setMaxPendingRefreshSchemaRequests(4)
        .setRefreshNodeListIntervalMillis(5)
        .setRefreshNodeIntervalMillis(6)
        .setRefreshSchemaIntervalMillis(7)
        .setReprepareOnUp(false)
        .setPrepareOnAllHosts(false)
        .setLoadBalancingLwtRequestRoutingMethod(RequestRoutingMethod.REGULAR)
        .setSchemaQueriesPaged(false);
  }

  private static void assertDiffers(QueryOptions changed) {
    QueryOptions defaults = new QueryOptions();
    assertThat(changed).isNotEqualTo(defaults);
    assertThat(defaults).isNotEqualTo(changed);
  }

  private static void assertIgnored(QueryOptions changed) {
    QueryOptions defaults = new QueryOptions();
    assertThat(changed).isEqualTo(defaults);
    assertThat(changed.hashCode()).isEqualTo(defaults.hashCode());
  }
}
