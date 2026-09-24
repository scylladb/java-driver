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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ErrorAwarePolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.HostFilterPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.PagingOptimizingLoadBalancingPolicy;
import com.datastax.driver.core.policies.PercentileSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.RackAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.testng.annotations.Test;

public class DefaultDriverConfigReporterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // The normative JSON Schema from the design doc, shipped verbatim as a test resource. Loaded once
  // and pinned to draft 2020-12 (its declared $schema); its internal "#/$defs/..." refs resolve
  // locally, so validation needs no network access.
  //
  // "v1" throughout is the report's "version" field, which the schema pins to 1. The design doc has
  // been revised several times without bumping it — only a key whose meaning changes does that, and
  // nothing has shipped yet — so the doc's revision is deliberately not a label here.
  private static final JsonSchema SCHEMA = loadSchema();

  // The one gap left between the 3.x configuration and the schema, carried only by a report of a
  // PercentileSpeculativeExecutionPolicy configured with a percentile of 0: the policy accepts it
  // and the schema's exclusiveMinimum does not. See should_report_a_zero_percentile_as_is and the
  // reporter's class javadoc.
  private static final String PERCENTILE_ZERO_GAP =
      "$.query.speculative-execution.policy.percentile: must have an exclusive minimum value of 0";

  private static final String SPECULATIVE_EXECUTION_POLICY_PATH =
      "$.query.speculative-execution.policy";

  private static JsonSchema loadSchema() {
    try (InputStream in =
        DefaultDriverConfigReporterTest.class.getResourceAsStream(
            "/config/driver-config-report-v1.schema.json")) {
      return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
          .getSchema(MAPPER.readTree(in));
    } catch (Exception e) {
      throw new AssertionError("Cannot load the DRIVER_CONFIG v1 JSON Schema resource", e);
    }
  }

  // ---- Stage 1: default / fail-safe -----------------------------------------------------------

  private static Configuration config() {
    return Configuration.builder().build();
  }

  @Test(groups = "unit")
  public void should_enable_driver_config_reporting_by_default() {
    assertThat(config().isDriverConfigReportingEnabled()).isTrue();
    assertThat(
            Cluster.builder()
                .addContactPoint("127.0.0.1")
                .getConfiguration()
                .isDriverConfigReportingEnabled())
        .isTrue();
  }

  @Test(groups = "unit")
  public void should_report_schema_version_and_config_groups() throws Exception {
    JsonNode report = MAPPER.readTree(new DefaultDriverConfigReporter(config()).buildReport());

    assertThat(report.path("version").asInt()).isEqualTo(1);
    assertThat(report.has("connection")).isTrue();
  }

  @Test(groups = "unit")
  public void should_be_fail_safe_when_report_build_throws() {
    DefaultDriverConfigReporter reporter =
        new DefaultDriverConfigReporter(config()) {
          @Override
          protected String buildJson() {
            throw new RuntimeException("boom");
          }
        };

    // Must not propagate the failure (it runs on the cluster-initialization path); no report means
    // no DRIVER_CONFIG option is sent, and nothing else about the connection is affected.
    assertThat(reporter.buildReport()).isNull();
  }

  @Test(groups = "unit")
  public void should_be_fail_safe_when_report_build_throws_internal_error() {
    DefaultDriverConfigReporter reporter =
        new DefaultDriverConfigReporter(config()) {
          @Override
          protected String buildJson() {
            // customPolicy() calls getClass().getSimpleName() on arbitrary user-supplied policy
            // objects, which has a documented JDK edge case throwing InternalError for certain
            // synthetic classes.
            throw new InternalError("simulated getSimpleName() JDK edge case");
          }
        };

    assertThat(reporter.buildReport()).isNull();
  }

  @Test(groups = "unit")
  public void should_build_the_report_through_the_connection_factory_guard() throws Exception {
    // Connection.Factory never touches DefaultDriverConfigReporter on the connection path: on a
    // classpath without Jackson, initializing that class raises a LinkageError from its static
    // ObjectMapper field -- an Error raised while initializing the class, so no fail-safe inside it
    // could catch it, and the Cluster would fail to initialize at all rather than merely skip the
    // report. The guard runs once, as the Cluster initializes, and decides whether any control
    // connection may build a report at all; on a normal classpath it is transparent, which is what
    // this pins.
    assertThat(Connection.Factory.canBuildDriverConfigReport(Cluster.builder().getConfiguration()))
        .isTrue();
  }

  @Test(groups = "unit")
  public void should_skip_driver_config_when_it_exceeds_the_size_limit() {
    // STARTUP option values are written with an unchecked 16-bit length prefix, so an oversized
    // report would corrupt the frame and fail the handshake rather than merely be useless. Parts of
    // the report come from unbounded user-supplied values (DC/rack names, consistency levels,
    // custom policy class names), so the limit has to be enforced here.
    assertThat(reporting(oversizedReport()).buildReport()).isNull();
  }

  @Test(groups = "unit")
  public void should_return_driver_config_that_is_just_within_the_size_limit() {
    String atLimit = padTo(DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH);

    assertThat(reporting(atLimit).buildReport()).isEqualTo(atLimit);
  }

  @Test(groups = "unit")
  public void should_report_default_configuration_within_the_size_limit() {
    // Tripwire: the real report is nowhere near the limit today. If it ever grows past it, this
    // fails loudly instead of DRIVER_CONFIG silently disappearing from the wire.
    String report =
        new DefaultDriverConfigReporter(Cluster.builder().getConfiguration()).buildReport();

    assertThat(report).isNotNull();
    assertThat(report.getBytes(StandardCharsets.UTF_8).length)
        .isLessThanOrEqualTo(DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH);
  }

  @Test(groups = "unit", timeOut = 30000)
  public void should_not_follow_a_cyclic_load_balancing_policy_chain_forever() throws Exception {
    // getChildPolicy() is walked on arbitrary user policies, so a cyclic chain would spin forever
    // on the cluster-initialization path. That is the one failure mode the reporter's try/catch
    // cannot contain, since it hangs rather than throws. The walk is bounded, so the report is
    // still produced, describing the outermost policy.
    JsonNode report =
        report(Cluster.builder().withLoadBalancingPolicy(new CyclicLoadBalancingPolicy()));

    assertThat(lbPolicy(report).path("type").asText()).isEqualTo("custom");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_name_the_configured_policy_not_the_internal_wrapper() throws Exception {
    // Cluster.Manager wraps every configured policy in PagingOptimizingLoadBalancingPolicy, so
    // naming the outermost policy of the chain would report that internal wrapper for every custom
    // policy, telling an operator nothing about what the client actually runs.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new PagingOptimizingLoadBalancingPolicy(new CustomLoadBalancingPolicy())));

    JsonNode policy = lbPolicy(report);
    assertThat(policy.path("type").asText()).isEqualTo("custom");
    assertThat(policy.path("name").asText()).isEqualTo("CustomLoadBalancingPolicy");
    assertConformsToSchema(report);
  }

  // ---- Stage 2: the full report ----------------------------------------------------------------

  @Test(groups = "unit")
  public void should_report_default_configuration_shape() throws Exception {
    JsonNode report = report(Cluster.builder());

    assertThat(report.path("version").asInt()).isEqualTo(1);
    // Everything hangs off exactly three groups: the schema keeps connection-scoped settings under
    // "connection", control-connection ones under "control-plane", and everything acting on a query
    // under "query".
    assertThat(fieldNames(report)).containsOnly("version", "connection", "control-plane", "query");

    // connection: connect + read only (no write timeout, no heartbeat in this schema version), plus
    // the per-connection request capacity, the pool, the socket and the reconnection policy.
    JsonNode connection = report.path("connection");
    assertThat(connection.path("connect").path("timeout-ms").asInt()).isEqualTo(5000);
    assertThat(connection.path("read").path("timeout-ms").asInt()).isEqualTo(12000);
    assertThat(connection.has("write")).isFalse();
    assertThat(connection.has("heartbeat")).isFalse();
    // Effective v3+ default; "orphaned" has no 3.x equivalent and is omitted.
    assertThat(connection.path("requests").path("in-flight").path("max").asInt()).isEqualTo(1024);
    assertThat(connection.path("requests").has("orphaned")).isFalse();
    // shard-aware is on by default via Cluster.Builder; the pool carries nothing else.
    assertThat(connection.path("pool").path("shard-aware").path("enabled").asBoolean()).isTrue();
    // The datacenter half of the node preference, which is what scopes pooling. Nothing is
    // configured by default, and nothing is inferred before the policy is initialized, so the
    // name is absent — but dc-auto is the accurate claim, the default policy being DC-aware.
    assertThat(connection.path("node-preference").path("type").asText()).isEqualTo("dc-auto");
    assertThat(connection.path("node-preference").has("local-dc")).isFalse();
    // TLS is off by default, and the group is absent rather than reporting "off".
    assertThat(connection.has("tls")).isFalse();

    // connection.socket: booleans present; buffers/linger omitted when unset.
    JsonNode socket = connection.path("socket");
    assertThat(socket.path("tcp-no-delay").asBoolean()).isTrue();
    assertThat(socket.path("keep-alive").asBoolean()).isFalse();
    assertThat(socket.path("reuse-address").asBoolean()).isFalse();
    assertThat(socket.has("linger")).isFalse();
    assertThat(socket.has("receive-buffer")).isFalse();
    assertThat(socket.has("send-buffer")).isFalse();

    // connection.reconnection: exponential, unbounded (no max-attempts).
    JsonNode reconnection = connection.path("reconnection").path("policy");
    assertThat(reconnection.path("type").asText()).isEqualTo("exponential");
    assertThat(reconnection.path("base-ms").asInt()).isEqualTo(1000);
    assertThat(reconnection.path("max-ms").asInt()).isEqualTo(600000);
    assertThat(reconnection.has("max-attempts")).isFalse();

    // control-plane.
    JsonNode systemTimeout =
        report.path("control-plane").path("queries").path("system").path("timeout");
    assertThat(systemTimeout.path("client-side-ms").asInt()).isEqualTo(12000);
    assertThat(systemTimeout.has("server-side-ms")).isFalse();
    assertThat(
            report
                .path("control-plane")
                .path("schema")
                .path("agreement")
                .path("timeout-ms")
                .asInt())
        .isEqualTo(10000);

    JsonNode query = report.path("query");

    // query.defaults.
    JsonNode defaults = query.path("defaults");
    assertThat(defaults.path("page").path("size").asInt()).isEqualTo(5000);
    assertThat(defaults.path("consistency").asText()).isEqualTo("LOCAL_ONE");
    assertThat(defaults.path("serial-consistency").asText()).isEqualTo("SERIAL");
    assertThat(defaults.path("idempotence").asBoolean()).isFalse();
    assertThat(defaults.path("client-timestamps").asBoolean()).isTrue();
    assertThat(defaults.path("request").path("timeout-ms").asInt()).isEqualTo(12000);

    // query.retry: no built-in 3.x retry policy delays its attempts, so there is no backoff, and
    // none bounds every one of its error paths, so there is no retry count either.
    assertThat(query.path("retry").path("policy").path("type").asText())
        .isEqualTo("standard-error-aware");
    assertThat(query.path("retry").has("backoff")).isFalse();
    assertThat(query.path("retry").path("policy").has("max-retries")).isFalse();

    // query.speculative-execution: absent, since there is none by default.
    assertThat(query.has("speculative-execution")).isFalse();

    // query.load-balancing: the default is token-aware over DC-aware (auto DC), with RANDOM replica
    // ordering.
    JsonNode lb = query.path("load-balancing").path("policy");
    assertThat(lb.path("type").asText()).isEqualTo("token-aware");
    assertThat(lb.path("load-distribution").asText()).isEqualTo("shuffle");
    assertThat(lb.path("fallback-to-non-preferred-nodes").asBoolean()).isFalse();
    // Nothing reorders candidates at runtime, and the group's "signals" cannot be empty, so it is
    // omitted rather than reported as disabled.
    assertThat(lb.has("adaptive-ordering")).isFalse();

    // query.load-balancing.node-preference: inferred DC, not yet resolved.
    JsonNode nodePreference = query.path("load-balancing").path("node-preference");
    assertThat(nodePreference.path("type").asText()).isEqualTo("dc-auto");
    assertThat(nodePreference.has("local-dc")).isFalse();
  }

  @Test(groups = "unit")
  public void should_report_constant_reconnection_policy() throws Exception {
    JsonNode report =
        report(Cluster.builder().withReconnectionPolicy(new ConstantReconnectionPolicy(2500)));

    JsonNode reconnection = report.path("connection").path("reconnection").path("policy");
    assertThat(reconnection.path("type").asText()).isEqualTo("constant");
    assertThat(reconnection.path("delay-ms").asInt()).isEqualTo(2500);
    assertThat(reconnection.has("max-attempts")).isFalse();
  }

  @Test(groups = "unit")
  public void should_discriminate_retry_policies() throws Exception {
    assertThat(retryPolicyType(Cluster.builder().withRetryPolicy(FallthroughRetryPolicy.INSTANCE)))
        .isEqualTo("fallthrough");

    JsonNode downgrading =
        retryPolicy(
            report(Cluster.builder().withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)));
    assertThat(downgrading.path("type").asText()).isEqualTo("downgrading-consistency");
    // This policy stops after one attempt on the errors it downgrades, but tries the next host
    // without a bound on request errors, so there is no single retry count to report.
    assertThat(downgrading.has("max-retries")).isFalse();
  }

  @Test(groups = "unit")
  public void should_report_constant_speculative_execution() throws Exception {
    JsonNode report =
        report(
            Cluster.builder()
                .withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(100L, 2)));

    JsonNode specEx = speculativeExecution(report);
    assertThat(specEx.path("type").asText()).isEqualTo("constant");
    assertThat(specEx.path("max-executions").asInt()).isEqualTo(2);
    assertThat(specEx.path("delay-ms").asLong()).isEqualTo(100L);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_percentile_speculative_execution() throws Exception {
    JsonNode report = report(Cluster.builder().withSpeculativeExecutionPolicy(percentile(99.0)));

    JsonNode specEx = speculativeExecution(report);
    assertThat(specEx.path("type").asText()).isEqualTo("percentile");
    assertThat(specEx.path("max-executions").asInt()).isEqualTo(3);
    assertThat(specEx.path("percentile").asDouble()).isEqualTo(99.0);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_a_custom_speculative_execution_policy_by_name() throws Exception {
    JsonNode report =
        report(Cluster.builder().withSpeculativeExecutionPolicy(new CustomSpeculativeExecution()));

    JsonNode specEx = speculativeExecution(report);
    assertThat(specEx.path("type").asText()).isEqualTo("custom");
    assertThat(specEx.path("name").asText()).isEqualTo("CustomSpeculativeExecution");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_explicit_datacenter_node_location_preference() throws Exception {
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new TokenAwarePolicy(
                        DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build())));

    JsonNode nodePreference = nodePreference(report);
    assertThat(nodePreference.path("type").asText()).isEqualTo("dc");
    assertThat(nodePreference.path("local-dc").asText()).isEqualTo("dc1");
    assertThat(nodePreference.has("local-rack")).isFalse();
    // The same preference scopes pooling, so it is reported under connection as well. A DC-aware
    // policy has nothing beyond the datacenter, so the two slots agree exactly here.
    assertThat(connectionNodePreference(report)).isEqualTo(nodePreference);
    // token-aware wrapper is still reflected in the policy next to it.
    assertThat(lbPolicy(report).path("type").asText()).isEqualTo("token-aware");
  }

  @Test(groups = "unit")
  public void should_unwrap_paging_optimizing_load_balancing_policy() throws Exception {
    // At runtime Cluster.Manager wraps the configured LB policy in a
    // PagingOptimizingLoadBalancingPolicy, so the reporter must unwrap it to recover the real
    // policy's flags and location preference (rather than reporting it as a custom policy).
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new PagingOptimizingLoadBalancingPolicy(
                        new TokenAwarePolicy(
                            DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()))));

    assertThat(lbPolicy(report).path("type").asText()).isEqualTo("token-aware");
    assertThat(lbPolicy(report).path("load-distribution").asText()).isEqualTo("shuffle");
    JsonNode nodePreference = nodePreference(report);
    assertThat(nodePreference.path("type").asText()).isEqualTo("dc");
    assertThat(nodePreference.path("local-dc").asText()).isEqualTo("dc1");
  }

  @Test(groups = "unit")
  public void should_report_rack_node_location_preference() throws Exception {
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new RackAwareRoundRobinPolicy("dc1", "rack1", 0, false, false, false)));

    JsonNode nodePreference = nodePreference(report);
    assertThat(nodePreference.path("type").asText()).isEqualTo("rack");
    assertThat(nodePreference.path("local-dc").asText()).isEqualTo("dc1");
    assertThat(nodePreference.path("local-rack").asText()).isEqualTo("rack1");
    // The rack never reaches the connection slot: a local-datacenter host in another rack
    // is REMOTE, not IGNORED, so it is still pooled and the rack scopes no pooling at all.
    // Reporting "rack" here would claim a restriction the driver does not apply.
    JsonNode connectionPreference = connectionNodePreference(report);
    assertThat(connectionPreference.path("type").asText()).isEqualTo("dc");
    assertThat(connectionPreference.path("local-dc").asText()).isEqualTo("dc1");
    assertThat(connectionPreference.has("local-rack")).isFalse();
    // Not token-aware, so the policy itself can only be reported by name.
    assertThat(lbPolicy(report).path("name").asText()).isEqualTo("RackAwareRoundRobinPolicy");
  }

  @Test(groups = "unit")
  public void should_report_configured_and_inferred_rack_location_under_separate_keys()
      throws Exception {
    // The datacenter is configured and the rack is left to be inferred, which the schema reports as
    // rack-auto with the configured part under local-dc — it admits only one key per part, and
    // rejects a configured DC and rack together (that is the "rack" type).
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new RackAwareRoundRobinPolicy("dc1", null, 0, false, false, true)));

    JsonNode nodePreference = nodePreference(report);
    assertThat(nodePreference.path("type").asText()).isEqualTo("rack-auto");
    assertThat(nodePreference.path("local-dc").asText()).isEqualTo("dc1");
    assertThat(nodePreference.has("inferred-local-dc")).isFalse();
    // Nothing has been inferred yet: the policy is only initialized once the cluster connects.
    assertThat(nodePreference.has("local-rack")).isFalse();
    assertThat(nodePreference.has("inferred-local-rack")).isFalse();
    // The connection slot takes the configured datacenter, and the pending rack leaves no trace in
    // it — rack-auto is a claim about routing only.
    JsonNode connectionPreference = connectionNodePreference(report);
    assertThat(connectionPreference.path("type").asText()).isEqualTo("dc");
    assertThat(connectionPreference.path("local-dc").asText()).isEqualTo("dc1");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_the_datacenter_the_policy_has_already_inferred() throws Exception {
    DCAwareRoundRobinPolicy policy = DCAwareRoundRobinPolicy.builder().build();
    Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(policy);

    // Cluster.Manager.init() builds the Connection.Factory, and the first control connection sends
    // its STARTUP, before it calls LoadBalancingPolicy#init -- so the report that first handshake
    // carries can only say that a datacenter will be inferred, not which one.
    JsonNode first = report(builder);
    assertThat(nodePreference(first).path("type").asText()).isEqualTo("dc-auto");
    assertThat(nodePreference(first).has("local-dc")).isFalse();
    assertThat(connectionNodePreference(first).has("local-dc")).isFalse();
    assertConformsToSchema(first);

    initWithNode(policy, "dc-inferred", "rack1");

    // Every later control connection builds its own report, so the datacenter the policy has since
    // inferred reaches the server on the first reconnect. Nothing was cached in between.
    JsonNode second = report(builder);
    assertThat(nodePreference(second).path("type").asText()).isEqualTo("dc-auto");
    assertThat(nodePreference(second).path("local-dc").asText()).isEqualTo("dc-inferred");
    assertThat(connectionNodePreference(second).path("local-dc").asText()).isEqualTo("dc-inferred");
    assertConformsToSchema(second);
  }

  @Test(groups = "unit")
  public void should_report_the_rack_the_policy_has_already_inferred() throws Exception {
    // Same for a rack-aware policy, which infers both halves -- and this is the case that puts the
    // schema's inferred-* keys on the wire at all: rack-auto names the inferred datacenter and rack
    // under their own keys, so that a consumer can tell them from configured ones.
    RackAwareRoundRobinPolicy policy =
        new RackAwareRoundRobinPolicy(null, null, 0, false, true, true);
    Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(policy);

    JsonNode first = report(builder);
    assertThat(nodePreference(first).path("type").asText()).isEqualTo("rack-auto");
    assertThat(nodePreference(first).has("inferred-local-dc")).isFalse();
    assertThat(nodePreference(first).has("inferred-local-rack")).isFalse();
    assertConformsToSchema(first);

    initWithNode(policy, "dc-inferred", "rack-inferred");

    JsonNode second = report(builder);
    assertThat(nodePreference(second).path("type").asText()).isEqualTo("rack-auto");
    assertThat(nodePreference(second).path("inferred-local-dc").asText()).isEqualTo("dc-inferred");
    assertThat(nodePreference(second).path("inferred-local-rack").asText())
        .isEqualTo("rack-inferred");
    assertThat(nodePreference(second).has("local-dc")).isFalse();
    assertThat(nodePreference(second).has("local-rack")).isFalse();
    // The connection slot takes the datacenter half only, and reports an inferred one under the
    // plain local-dc key -- dc-auto has no inferred- prefix, which is the schema's own asymmetry.
    assertThat(connectionNodePreference(second).path("type").asText()).isEqualTo("dc-auto");
    assertThat(connectionNodePreference(second).path("local-dc").asText()).isEqualTo("dc-inferred");
    assertConformsToSchema(second);
  }

  @Test(groups = "unit")
  public void should_report_server_side_timestamps_as_disabled_client_timestamps()
      throws Exception {
    JsonNode report =
        report(Cluster.builder().withTimestampGenerator(ServerSideTimestampGenerator.INSTANCE));

    assertThat(queryDefaults(report).path("client-timestamps").asBoolean()).isFalse();
  }

  @Test(groups = "unit")
  public void should_omit_client_timestamps_for_a_custom_timestamp_generator() throws Exception {
    // next() returning Long.MIN_VALUE is documented as "let Cassandra generate the timestamp", and
    // a custom generator decides that per call — so whether timestamps are assigned client-side is
    // not a property of the configuration at all. Optional, and documented as absent exactly when
    // the behavior is unknown, so it is omitted rather than guessed from the class.
    JsonNode report = report(Cluster.builder().withTimestampGenerator(new CustomTimestamps()));

    assertThat(queryDefaults(report).has("client-timestamps")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_client_timestamps_for_both_built_in_monotonic_generators()
      throws Exception {
    // Neither of the two can return Long.MIN_VALUE, so both always assign the timestamp
    // client-side. The default is the atomic one, pinned by
    // should_report_default_configuration_shape.
    for (TimestampGenerator generator :
        new TimestampGenerator[] {
          new AtomicMonotonicTimestampGenerator(), new ThreadLocalMonotonicTimestampGenerator()
        }) {
      JsonNode report = report(Cluster.builder().withTimestampGenerator(generator));

      assertThat(queryDefaults(report).path("client-timestamps").asBoolean()).isTrue();
      assertConformsToSchema(report);
    }
  }

  @Test(groups = "unit")
  public void should_report_the_configured_page_size() throws Exception {
    JsonNode report =
        report(Cluster.builder().withQueryOptions(new QueryOptions().setFetchSize(1234)));

    assertThat(queryDefaults(report).path("page").path("size").asInt()).isEqualTo(1234);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_omit_the_page_size_when_paging_is_disabled() throws Exception {
    // Integer.MAX_VALUE is how QueryOptions#setFetchSize documents "disable paging", and it is the
    // only way a session can end up unpaged: the setter rejects anything <= 0. The schema says page
    // is absent when paging is not limited, so the group goes rather than carrying the sentinel as
    // if it were a page size somebody chose.
    JsonNode report =
        report(
            Cluster.builder().withQueryOptions(new QueryOptions().setFetchSize(Integer.MAX_VALUE)));

    assertThat(queryDefaults(report).has("page")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_a_serial_default_consistency() throws Exception {
    // QueryOptions accepts a serial level as the default *request* consistency, which is legal for
    // a serial read, and the schema's enum admits both of them — so this needs no special handling
    // and the document stays valid. It used to be one of the gaps the reporter knowingly violated.
    for (ConsistencyLevel level :
        new ConsistencyLevel[] {ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL}) {
      JsonNode report =
          report(Cluster.builder().withQueryOptions(new QueryOptions().setConsistencyLevel(level)));

      assertThat(queryDefaults(report).path("consistency").asText()).isEqualTo(level.name());
      assertConformsToSchema(report);
    }
  }

  @Test(groups = "unit")
  public void should_omit_a_non_serial_default_serial_consistency() throws Exception {
    // QueryOptions, unlike Statement, does not check that the level is serial, so a non-serial one
    // reaches the reporter. serial-consistency is optional, so it is omitted rather than emitted as
    // a value the schema's SERIAL/LOCAL_SERIAL enum rejects.
    JsonNode report =
        report(
            Cluster.builder()
                .withQueryOptions(
                    new QueryOptions().setSerialConsistencyLevel(ConsistencyLevel.QUORUM)));

    assertThat(queryDefaults(report).has("serial-consistency")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_reject_a_null_default_consistency() {
    // Every query needs a level, so a null default fails any statement that does not set one of
    // its own: RoundRobinPolicy and DCAwareRoundRobinPolicy call isDCLocal() on it while building
    // a query plan. Rejecting it here turns a driver that breaks on its first query into one that
    // breaks while being configured.
    try {
      new QueryOptions().setConsistencyLevel(null);
      fail("Expected a NullPointerException");
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("consistencyLevel cannot be null");
    }
  }

  @Test(groups = "unit")
  public void should_omit_a_default_consistency_that_a_subclass_nulls_out() throws Exception {
    // With the setter rejecting null, only a QueryOptions subclass overriding the getter can still
    // hand the reporter one. The key stays omitted even though the schema requires it — that beats
    // losing every other group to an NPE caught by the fail-safe.
    QueryOptions queryOptions =
        new QueryOptions() {
          @Override
          public ConsistencyLevel getConsistencyLevel() {
            return null;
          }
        };
    JsonNode report = report(Cluster.builder().withQueryOptions(queryOptions));

    assertThat(queryDefaults(report).has("consistency")).isFalse();
    assertConformsToSchema(report, "$.query.defaults: required property 'consistency' not found");
  }

  @Test(groups = "unit")
  public void should_report_tls_only_when_it_is_enabled() throws Exception {
    // The group carries no "enabled" flag: its presence is what says TLS is on, so with TLS off it
    // is omitted rather than reported as disabled.
    assertThat(report(Cluster.builder()).path("connection").has("tls")).isFalse();

    JsonNode tls = report(Cluster.builder().withSSL()).path("connection").path("tls");
    assertThat(tls.isObject()).isTrue();
  }

  @Test(groups = "unit")
  public void should_report_hostname_verification_for_sni_ssl_options() throws Exception {
    // SniSSLOptions is the one 3.x SSLOptions that verifies the hostname, and it hard-codes it on,
    // so it is the only case the reporter can state.
    JsonNode report = report(Cluster.builder().withSSL(SniSSLOptions.builder().build()));

    assertThat(report.path("connection").path("tls").path("hostname-verification").asBoolean())
        .isTrue();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_omit_hostname_verification_it_cannot_observe() throws Exception {
    // withSSL() installs RemoteEndpointAwareJdkSSLOptions, which builds its engine from a user
    // SSLContext without touching endpoint identification; NettySSLOptions hands the whole handler
    // to Netty. Neither can be read for it, so the key is omitted rather than asserted false — the
    // schema documents it as absent exactly when the behavior is unknown. The group itself stays,
    // since its presence is what reports that TLS is on.
    JsonNode tls = report(Cluster.builder().withSSL()).path("connection").path("tls");

    assertThat(tls.isObject()).isTrue();
    assertThat(tls.has("hostname-verification")).isFalse();
    assertThat(fieldNames(tls)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_report_socket_overrides() throws Exception {
    SocketOptions socketOptions =
        new SocketOptions()
            .setKeepAlive(true)
            .setReuseAddress(true)
            .setSoLinger(15)
            .setReceiveBufferSize(4096)
            .setSendBufferSize(8192);

    JsonNode socket = socket(report(Cluster.builder().withSocketOptions(socketOptions)));

    assertThat(socket.path("keep-alive").asBoolean()).isTrue();
    assertThat(socket.path("reuse-address").asBoolean()).isTrue();
    assertThat(socket.path("linger").path("interval-s").asInt()).isEqualTo(15);
    assertThat(socket.path("receive-buffer").path("size-bytes").asInt()).isEqualTo(4096);
    assertThat(socket.path("send-buffer").path("size-bytes").asInt()).isEqualTo(8192);
  }

  @Test(groups = "unit")
  public void should_report_fallback_to_non_preferred_nodes_when_remote_hosts_are_used()
      throws Exception {
    // Leaving the preferred datacenter is the only way a DC-aware policy reaches a node outside
    // the reported node-preference, so the flag follows usedHostsPerRemoteDc. It needs a
    // token-aware wrapper to be reported at all: that is the only built-in shape the schema
    // defines, and the only one carrying the flag. The false case is pinned by
    // should_report_default_configuration_shape.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new TokenAwarePolicy(
                        DCAwareRoundRobinPolicy.builder()
                            .withLocalDc("dc1")
                            .withUsedHostsPerRemoteDc(2)
                            .build())));

    assertThat(lbPolicy(report).path("fallback-to-non-preferred-nodes").asBoolean()).isTrue();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_always_report_fallback_to_non_preferred_nodes_for_a_rack_aware_policy()
      throws Exception {
    // A rack-aware policy reports a rack preference, and the other racks of its local datacenter
    // are outside it yet are the second tier of every query plan — distance() returns REMOTE, not
    // IGNORED, for them. So the flag is true even with no remote datacenter host configured, which
    // is the default.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new TokenAwarePolicy(
                        new RackAwareRoundRobinPolicy("dc1", "rack1", 0, false, false, false))));

    assertThat(nodePreference(report).path("type").asText()).isEqualTo("rack");
    assertThat(lbPolicy(report).path("fallback-to-non-preferred-nodes").asBoolean()).isTrue();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_fallback_to_non_preferred_nodes_as_false_without_a_preference()
      throws Exception {
    // A token-aware policy over a child with no locality notion has no preference to report, so the
    // flag has nothing to be relative to: the schema defines it against
    // query.load-balancing.node-preference, and that key is absent here. The flag is required on
    // the
    // token-aware branch, so it has to carry some value; false is reported because RoundRobinPolicy
    // has no tiering to fall back *from*, not because anything is restricted. Pinned so the pairing
    // cannot change silently while it is an open question for the schema owner.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())));

    assertThat(lbPolicy(report).path("type").asText()).isEqualTo("token-aware");
    assertThat(lbPolicy(report).path("fallback-to-non-preferred-nodes").asBoolean()).isFalse();
    assertThat(loadBalancing(report).has("node-preference")).isFalse();
    assertThat(report.path("connection").has("node-preference")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_the_configured_in_flight_request_limit() throws Exception {
    JsonNode report =
        report(
            Cluster.builder()
                .withPoolingOptions(
                    new PoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 512)));

    assertThat(report.path("connection").path("requests").path("in-flight").path("max").asInt())
        .isEqualTo(512);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_a_configured_zero_in_flight_request_limit_as_is() throws Exception {
    // PoolingOptions rejects only negatives, so 0 is a limit an operator can set deliberately.
    // Falling back to the protocol default for it would misreport the pool; the key is required and
    // positive-only, so it is reported as-is and shows up as a violation instead. The other end
    // needs no such treatment — see should_report_the_maximal_in_flight_request_limit.
    JsonNode report =
        report(
            Cluster.builder()
                .withPoolingOptions(
                    new PoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 0)));

    assertThat(report.path("connection").path("requests").path("in-flight").path("max").asInt())
        .isEqualTo(0);
    assertConformsToSchema(
        report, "$.connection.requests.in-flight.max: must have a minimum value of 1");
  }

  @Test(groups = "unit")
  public void should_report_the_maximal_in_flight_request_limit() throws Exception {
    // The largest limit PoolingOptions accepts is the 32768 stream identifiers protocol v3
    // provides, and the schema bounds the key only from below, so the largest legal configuration
    // is still a valid document.
    JsonNode report =
        report(
            Cluster.builder()
                .withPoolingOptions(
                    new PoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)));

    assertThat(report.path("connection").path("requests").path("in-flight").path("max").asInt())
        .isEqualTo(32768);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_the_in_flight_request_limit_of_the_pinned_protocol_version()
      throws Exception {
    // PoolingOptions is still UNSET when the report is built, so the limit comes from the DEFAULTS
    // row the pools will eventually be sized from. That row is not always the v3 one: a user who
    // pinned the protocol version gets the highest row not above it, and DEFAULTS only has v1 and
    // v3
    // entries, so a v2 cluster is sized from v1's 128. Pinning the version is the one part of
    // negotiation that is knowable at report time.
    JsonNode report = report(Cluster.builder().withProtocolVersion(ProtocolVersion.V2));

    assertThat(report.path("connection").path("requests").path("in-flight").path("max").asInt())
        .isEqualTo(128);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_omit_the_orphaned_request_limit_it_has_no_equivalent_for() throws Exception {
    // A request the 3.x driver stopped waiting for keeps its stream identifier until the response
    // arrives; there is no configurable bound on those and no connection replacement, so there is
    // nothing to report. The group is optional as of the schema revision that added "absent only
    // when this bound is unknown", so omitting it is now the schema-valid answer rather than the
    // one violation every report carried — pinned here so the key cannot silently come back.
    JsonNode report = report(Cluster.builder());

    JsonNode requests = report.path("connection").path("requests");
    assertThat(requests.has("in-flight")).isTrue();
    assertThat(requests.has("orphaned")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_load_distribution_from_the_replica_ordering() throws Exception {
    assertThat(loadDistribution(TokenAwarePolicy.ReplicaOrdering.RANDOM)).isEqualTo("shuffle");
    assertThat(loadDistribution(TokenAwarePolicy.ReplicaOrdering.TOPOLOGICAL))
        .isEqualTo("replica-set");
    // NEUTRAL keeps the child policy's plan order, which for every built-in child rotates the first
    // host across successive query plans.
    assertThat(loadDistribution(TokenAwarePolicy.ReplicaOrdering.NEUTRAL)).isEqualTo("round-robin");
  }

  @Test(groups = "unit")
  public void should_report_latency_awareness_as_adaptive_ordering() throws Exception {
    // The wrapper contributes a capability, not a type: the type still comes from the token-aware
    // policy it wraps.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    LatencyAwarePolicy.builder(
                            new TokenAwarePolicy(
                                DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()))
                        .build()));

    JsonNode lb = lbPolicy(report);
    assertThat(lb.path("type").asText()).isEqualTo("token-aware");
    JsonNode adaptiveOrdering = lb.path("adaptive-ordering");
    // The group has no "enabled" flag; its presence is what says candidates get reordered, and its
    // signals cannot be empty.
    assertThat(fieldNames(adaptiveOrdering)).containsOnly("signals");
    assertThat(adaptiveOrdering.path("signals").size()).isEqualTo(1);
    assertThat(adaptiveOrdering.path("signals").get(0).asText()).isEqualTo("latency");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_a_built_in_policy_that_is_not_token_aware_as_custom() throws Exception {
    // "token-aware" is the only built-in shape the schema defines, so any other policy can only be
    // reported by name — its datacenter still shows up in the node preference beside it.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()));

    JsonNode lb = lbPolicy(report);
    assertThat(lb.path("type").asText()).isEqualTo("custom");
    assertThat(lb.path("name").asText()).isEqualTo("DCAwareRoundRobinPolicy");
    assertThat(nodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_keep_reporting_token_aware_for_a_fully_representable_chain() throws Exception {
    // The default chain, and every chain built only from policies the group can describe: token
    // awareness is claimed, and no name appears -- the built-in shape has no room for one.
    for (LoadBalancingPolicy policy :
        new LoadBalancingPolicy[] {
          new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()),
          new TokenAwarePolicy(new RoundRobinPolicy()),
          LatencyAwarePolicy.builder(
                  new TokenAwarePolicy(
                      DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()))
              .build(),
          // The internal wrapper Cluster.Manager adds describes nothing, so it does not cost the
          // chain its built-in shape any more than it earns a mention in a name.
          new PagingOptimizingLoadBalancingPolicy(
              new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build())),
          // A filter naming one datacenter is described by the node preference it produces.
          HostFilterPolicy.fromDCWhiteList(
              new TokenAwarePolicy(new RoundRobinPolicy()), Collections.singletonList("dc1"))
        }) {
      JsonNode report = report(Cluster.builder().withLoadBalancingPolicy(policy));

      assertThat(lbPolicy(report).path("type").asText()).as("%s", policy).isEqualTo("token-aware");
      assertThat(lbPolicy(report).has("name")).as("%s", policy).isFalse();
      assertConformsToSchema(report);
    }
  }

  @Test(groups = "unit")
  public void should_name_a_restricting_wrapper_over_a_token_aware_chain() throws Exception {
    // The bug this closes: an outer restriction used to vanish behind an inner TokenAwarePolicy, so
    // whether an operator could see that the client is pinned to a host list depended on an
    // unrelated nesting choice -- WhiteListPolicy(RoundRobinPolicy) reported the wrapper by name
    // while WhiteListPolicy(TokenAwarePolicy(...)) reported plain token-aware. Now the chain that
    // cannot be described as a built-in says so, and names every policy in it.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new WhiteListPolicy(
                        new TokenAwarePolicy(
                            DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()),
                        Collections.singletonList(new InetSocketAddress("127.0.0.1", 9042)))));

    JsonNode lb = lbPolicy(report);
    assertThat(lb.path("type").asText()).isEqualTo("custom");
    assertThat(lb.path("name").asText())
        .isEqualTo("WhiteListPolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy))");
    // Naming the wrapper costs nothing that was reported before it: what the chain can still state
    // about itself stays, as additional properties the custom branch admits.
    assertThat(lb.path("load-distribution").asText()).isEqualTo("shuffle");
    assertThat(lb.path("fallback-to-non-preferred-nodes").asBoolean()).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_name_an_error_aware_wrapper_that_has_no_other_representation()
      throws Exception {
    // ErrorAwarePolicy excludes hosts over an error-rate threshold -- a restriction, not a
    // reordering -- so adaptive-ordering would be the wrong home for it despite its inviting
    // response-rate signal. The name is the only place it can appear, and now it does.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    ErrorAwarePolicy.builder(
                            new TokenAwarePolicy(
                                DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()))
                        .build()));

    JsonNode lb = lbPolicy(report);
    assertThat(lb.path("type").asText()).isEqualTo("custom");
    assertThat(lb.path("name").asText())
        .isEqualTo("ErrorAwarePolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy))");
    assertThat(lb.has("adaptive-ordering")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_an_anonymous_custom_policy_under_its_binary_name() throws Exception {
    // An anonymous class has no simple name, and the schema requires a non-empty one.
    JsonNode report =
        report(Cluster.builder().withLoadBalancingPolicy(new CustomLoadBalancingPolicy() {}));

    assertThat(lbPolicy(report).path("name").asText())
        .startsWith(DefaultDriverConfigReporterTest.class.getName() + "$");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_omit_every_field_a_disabled_read_timeout_feeds() throws Exception {
    // A non-positive read timeout disables read timeouts. It feeds three places, all optional and
    // positive-only, so all three are omitted rather than emitted as a value the schema rejects.
    JsonNode report =
        report(Cluster.builder().withSocketOptions(new SocketOptions().setReadTimeoutMillis(0)));

    assertThat(report.path("connection").has("read")).isFalse();
    // The enclosing "timeout" object here is required, so it stays behind, empty.
    JsonNode systemTimeout =
        report.path("control-plane").path("queries").path("system").path("timeout");
    assertThat(systemTimeout.isObject()).isTrue();
    assertThat(systemTimeout.has("client-side-ms")).isFalse();
    // "request" is optional, so the whole group goes rather than being reported empty.
    assertThat(queryDefaults(report).has("request")).isFalse();

    // Nothing beyond the gap every report carries: making the request timeout and its group
    // optional
    // removed the one violation this configuration used to add.
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_omit_a_disabled_connect_timeout() throws Exception {
    // Non-positive means "no timeout". The timeout is optional now, so it is omitted rather than
    // reported as a value the schema rejects; its enclosing group is required, so it stays (empty).
    JsonNode connect =
        report(Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(0)))
            .path("connection")
            .path("connect");

    assertThat(connect.isObject()).isTrue();
    assertThat(connect.has("timeout-ms")).isFalse();
  }

  @Test(groups = "unit")
  public void should_omit_socket_values_the_schema_cannot_express() throws Exception {
    // A negative SO_LINGER disables lingering close and a non-positive buffer size leaves the
    // JDK/OS default in place; all three groups are optional, so they are omitted.
    JsonNode socket =
        socket(
            report(
                Cluster.builder()
                    .withSocketOptions(
                        new SocketOptions()
                            .setSoLinger(-1)
                            .setReceiveBufferSize(0)
                            .setSendBufferSize(-1))));

    assertThat(socket.has("linger")).isFalse();
    assertThat(socket.has("receive-buffer")).isFalse();
    assertThat(socket.has("send-buffer")).isFalse();
  }

  @Test(groups = "unit")
  public void should_report_a_zero_linger_interval() throws Exception {
    // 0 means "close immediately, discarding unsent data", which the schema does admit.
    JsonNode report =
        report(Cluster.builder().withSocketOptions(new SocketOptions().setSoLinger(0)));

    assertThat(socket(report).path("linger").path("interval-s").asInt()).isEqualTo(0);
    assertConformsToSchema(report);
  }

  // ==================== Schema conformance ====================
  //
  // These build a config, serialize it via the reporter, and validate the produced JSON against the
  // normative JSON Schema (the same document ScyllaDB uses to interpret DRIVER_CONFIG). They cover
  // every discriminated-union branch and optional-group case the 3.x reporter can emit, turning the
  // "no emitted document violates the schema beyond the documented gaps" invariant into an enforced
  // test.
  //
  // There is one documented gap left, PERCENTILE_ZERO_GAP, which only a percentile of 0 provokes;
  // see the reporter's class javadoc and should_report_a_zero_percentile_as_is, which pins it.

  @Test(groups = "unit")
  public void should_conform_to_schema_for_default_report() throws Exception {
    assertConformsToSchema(report(Cluster.builder()));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_constant_reconnection_policy() throws Exception {
    assertConformsToSchema(
        report(Cluster.builder().withReconnectionPolicy(new ConstantReconnectionPolicy(2500))));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_zero_delay_constant_reconnection_policy()
      throws Exception {
    // A zero delay means "reconnect immediately", which the schema admits.
    JsonNode report =
        report(Cluster.builder().withReconnectionPolicy(new ConstantReconnectionPolicy(0)));

    assertThat(
            report.path("connection").path("reconnection").path("policy").path("delay-ms").asInt())
        .isEqualTo(0);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_a_disabled_connect_timeout() throws Exception {
    assertConformsToSchema(
        report(
            Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(0))));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_custom_reconnection_policy() throws Exception {
    assertConformsToSchema(
        report(Cluster.builder().withReconnectionPolicy(new CustomReconnectionPolicy())));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_custom_retry_policy() throws Exception {
    assertConformsToSchema(
        report(
            Cluster.builder()
                .withRetryPolicy(new LoggingRetryPolicy(FallthroughRetryPolicy.INSTANCE))));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_downgrading_consistency_retry_policy() throws Exception {
    assertConformsToSchema(
        report(Cluster.builder().withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_fallthrough_retry_policy() throws Exception {
    assertConformsToSchema(
        report(Cluster.builder().withRetryPolicy(FallthroughRetryPolicy.INSTANCE)));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_a_zero_delay_constant_speculative_execution()
      throws Exception {
    // A zero delay means every extra execution is sent immediately, which the schema's
    // nonNegativeInteger admits.
    JsonNode report =
        report(
            Cluster.builder()
                .withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(0L, 1)));

    assertThat(speculativeExecution(report).path("delay-ms").asLong()).isEqualTo(0L);
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_a_zero_percentile_as_is() throws Exception {
    // PercentileSpeculativeExecutionPolicy accepts a percentile of 0, which the schema's
    // exclusiveMinimum rejects, and the key is required by the percentile branch. Reported as-is;
    // see the reporter's class javadoc.
    JsonNode report = report(Cluster.builder().withSpeculativeExecutionPolicy(percentile(0.0)));

    assertThat(speculativeExecution(report).path("percentile").asDouble()).isEqualTo(0.0);

    Set<String> violations = violations(report);
    assertThat(violations).contains(PERCENTILE_ZERO_GAP);
    // Failing the percentile branch drops the object out of the discriminated union, so the
    // validator goes on to explain why it matches neither of the other two branches either. Those
    // messages all follow from this one gap, so rather than pin the cascade, assert that it stays
    // inside this one object — nothing else in the report is affected.
    for (String violation : violations) {
      assertThat(violation).startsWith(SPECULATIVE_EXECUTION_POLICY_PATH);
    }
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_explicit_datacenter_node_location() throws Exception {
    assertConformsToSchema(
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new TokenAwarePolicy(
                        DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()))));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_rack_node_location() throws Exception {
    assertConformsToSchema(
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new RackAwareRoundRobinPolicy("dc1", "rack1", 0, false, false, false))));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_inferred_rack_node_location() throws Exception {
    // Neither DC nor rack configured: reported as rack-auto, with both names absent because the
    // policy has not been initialized (and so has inferred nothing) at report time.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new RackAwareRoundRobinPolicy(null, null, 0, false, true, true)));

    assertThat(nodePreference(report).path("type").asText()).isEqualTo("rack-auto");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_configured_rack_and_inferred_datacenter()
      throws Exception {
    // The mirror of should_report_configured_and_inferred_rack_location_under_separate_keys: the
    // configured part is the rack this time, which exercises the schema's "not local-dc together
    // with local-rack" guard from the other side.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new RackAwareRoundRobinPolicy(null, "rack1", 0, false, true, false)));

    JsonNode nodePreference = nodePreference(report);
    assertThat(nodePreference.path("type").asText()).isEqualTo("rack-auto");
    assertThat(nodePreference.path("local-rack").asText()).isEqualTo("rack1");
    assertThat(nodePreference.has("local-dc")).isFalse();
    // The connection slot degrades to dc-auto with no name: the configured rack says nothing about
    // which datacenter is pooled, and the datacenter itself is still to be inferred.
    JsonNode connectionPreference = connectionNodePreference(report);
    assertThat(connectionPreference.path("type").asText()).isEqualTo("dc-auto");
    assertThat(connectionPreference.has("local-dc")).isFalse();
    assertThat(connectionPreference.has("local-rack")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_round_robin_load_balancing_policy() throws Exception {
    JsonNode report = report(Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy()));

    assertThat(lbPolicy(report).path("name").asText()).isEqualTo("RoundRobinPolicy");
    // No DC/rack notion at all, so neither of the schema's two node preference keys is reported.
    assertThat(loadBalancing(report).has("node-preference")).isFalse();
    assertThat(report.path("connection").has("node-preference")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_white_list_load_balancing_policy() throws Exception {
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new WhiteListPolicy(
                        new RoundRobinPolicy(),
                        Collections.singletonList(new InetSocketAddress("127.0.0.1", 9042)))));

    assertThat(lbPolicy(report).path("name").asText())
        .isEqualTo("WhiteListPolicy(RoundRobinPolicy)");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_the_preferred_datacenter_under_a_filtering_wrapper() throws Exception {
    // A filtering wrapper narrows pooling below the datacenter reported beside it: WhiteListPolicy
    // extends HostFilterPolicy, whose distance() returns IGNORED for any host failing the
    // predicate, including one inside dc1. The configured datacenter is reported anyway -- hiding a
    // setting the operator really did make is the worse failure mode -- so this pins the
    // approximation the class javadoc documents rather than leaving the shape to change silently.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new WhiteListPolicy(
                        DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build(),
                        Collections.singletonList(new InetSocketAddress("127.0.0.1", 9042)))));

    // The whitelist has no home in the schema's built-in shape, so the chain cannot claim it; the
    // name says the whole of what the client runs rather than only its outermost policy.
    assertThat(lbPolicy(report).path("type").asText()).isEqualTo("custom");
    assertThat(lbPolicy(report).path("name").asText())
        .isEqualTo("WhiteListPolicy(DCAwareRoundRobinPolicy)");
    // Both preference slots still carry the datacenter of the DC-aware child inside the chain.
    assertThat(nodePreference(report).path("type").asText()).isEqualTo("dc");
    assertThat(nodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertThat(connectionNodePreference(report).path("type").asText()).isEqualTo("dc");
    assertThat(connectionNodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_host_filter_load_balancing_policy() throws Exception {
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    HostFilterPolicy.fromDCWhiteList(
                        new RoundRobinPolicy(), Collections.singletonList("dc1"))));

    assertThat(lbPolicy(report).path("name").asText())
        .isEqualTo("HostFilterPolicy(RoundRobinPolicy)");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_report_the_datacenter_a_filter_restricts_the_session_to() throws Exception {
    // fromDCWhiteList over a RoundRobinPolicy is the one restriction the driver ships that names a
    // datacenter without any policy in the chain preferring one. Its distance() returns IGNORED for
    // every host outside dc1, so the restriction really is what decides which hosts are pooled and
    // which appear in a query plan -- both preference slots, in schema terms.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    HostFilterPolicy.fromDCWhiteList(
                        new RoundRobinPolicy(), Collections.singletonList("dc1"))));

    assertThat(nodePreference(report).path("type").asText()).isEqualTo("dc");
    assertThat(nodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertThat(connectionNodePreference(report).path("type").asText()).isEqualTo("dc");
    assertThat(connectionNodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_prefer_a_location_aware_policy_over_the_filter_above_it() throws Exception {
    // The filter and the policy disagree here, which is a misconfiguration -- but the policy is
    // what builds the query plan, so its datacenter is the one reported. The filter is only ever
    // consulted when nothing in the chain prefers a datacenter of its own.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    HostFilterPolicy.fromDCWhiteList(
                        DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build(),
                        Collections.singletonList("dc2"))));

    assertThat(nodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertThat(connectionNodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    // The filter names a datacenter, but not the one reported -- so it restricts the session
    // without being stated anywhere, and the chain cannot claim to be fully described. Pinned
    // beside the token-aware case below, which is the same chain with one more wrapper.
    assertThat(lbPolicy(report).path("type").asText()).isEqualTo("custom");
    assertThat(lbPolicy(report).path("name").asText())
        .isEqualTo("HostFilterPolicy(DCAwareRoundRobinPolicy)");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_name_a_datacenter_filter_that_disagrees_with_the_policy_below_it()
      throws Exception {
    // The chain of should_prefer_a_location_aware_policy_over_the_filter_above_it plus a
    // token-aware wrapper, which must not make the filter disappear. A filter is only described by
    // the node preference when that preference is *its* restriction; here the DC-aware policy owns
    // it, so the whitelist narrows the session on top of what is reported -- to a datacenter
    // disjoint from it, in fact, leaving nothing reachable at all. Claiming the built-in
    // token-aware shape would say none of that, and would say it only because of the wrapper: the
    // same nesting-dependent blind spot should_name_a_restricting_wrapper_over_a_token_aware_chain
    // closes for WhiteListPolicy.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new TokenAwarePolicy(
                        HostFilterPolicy.fromDCWhiteList(
                            DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build(),
                            Collections.singletonList("dc2")))));

    JsonNode lb = lbPolicy(report);
    assertThat(lb.path("type").asText()).isEqualTo("custom");
    assertThat(lb.path("name").asText())
        .isEqualTo("TokenAwarePolicy(HostFilterPolicy(DCAwareRoundRobinPolicy))");
    // What the chain can still state about itself stays, as additional properties.
    assertThat(lb.path("load-distribution").asText()).isEqualTo("shuffle");
    assertThat(lb.path("fallback-to-non-preferred-nodes").asBoolean()).isFalse();
    // The preference is still the policy's, which is what builds the query plan.
    assertThat(nodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertThat(connectionNodePreference(report).path("local-dc").asText()).isEqualTo("dc1");
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_omit_the_node_preference_for_restrictions_that_name_no_single_datacenter()
      throws Exception {
    // Everything a filter can express other than "exactly these hosts, in exactly this one
    // datacenter". A blacklist denies a datacenter without naming a preferred one; two allowed
    // datacenters name no single one; a WhiteListPolicy filters on addresses, and a caller-supplied
    // predicate on whatever it likes. None of those has a schema-valid form, so the group goes.
    // Nor does a blank name: nothing validates the strings fromDCWhiteList is handed, and local-dc
    // is a non-empty string the "dc" type requires -- so the whole group goes rather than one key,
    // the same way the DC- and rack-aware policies treat a blank name of their own.
    LoadBalancingPolicy[] policies = {
      HostFilterPolicy.fromDCWhiteList(new RoundRobinPolicy(), Arrays.asList("dc1", "dc2")),
      HostFilterPolicy.fromDCBlackList(new RoundRobinPolicy(), Collections.singletonList("dc2")),
      HostFilterPolicy.fromDCWhiteList(new RoundRobinPolicy(), Collections.singletonList("")),
      new HostFilterPolicy(
          new RoundRobinPolicy(),
          new Predicate<Host>() {
            @Override
            public boolean apply(Host host) {
              return true;
            }
          }),
      new WhiteListPolicy(
          new RoundRobinPolicy(),
          Collections.singletonList(new InetSocketAddress("127.0.0.1", 9042)))
    };

    for (LoadBalancingPolicy policy : policies) {
      JsonNode report = report(Cluster.builder().withLoadBalancingPolicy(policy));

      assertThat(loadBalancing(report).has("node-preference")).as("%s", policy).isFalse();
      assertThat(report.path("connection").has("node-preference")).as("%s", policy).isFalse();
      assertConformsToSchema(report);
    }
  }

  @Test(groups = "unit")
  public void should_report_adaptive_ordering_without_token_awareness() throws Exception {
    // Adaptive ordering is a property of the chain, not of token awareness: LatencyAwarePolicy
    // reorders the candidates its child produces whatever that child is, so it is reported over a
    // bare RoundRobinPolicy exactly as it is over a token-aware policy. The custom branch admits
    // additional properties, which is what lets the capability be stated next to the name.
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    LatencyAwarePolicy.builder(new RoundRobinPolicy()).build()));

    JsonNode lb = lbPolicy(report);
    assertThat(lb.path("type").asText()).isEqualTo("custom");
    assertThat(lb.path("name").asText()).isEqualTo("LatencyAwarePolicy(RoundRobinPolicy)");
    assertThat(lb.path("adaptive-ordering").path("signals").get(0).asText()).isEqualTo("latency");
    // No token-aware policy in the chain, so neither key that describes one is claimed.
    assertThat(lb.has("load-distribution")).isFalse();
    assertThat(lb.has("fallback-to-non-preferred-nodes")).isFalse();
    assertConformsToSchema(report);
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_unwrapped_paging_optimizing_load_balancing_policy()
      throws Exception {
    assertConformsToSchema(
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new PagingOptimizingLoadBalancingPolicy(
                        new TokenAwarePolicy(
                            DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build())))));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_tls_enabled() throws Exception {
    assertConformsToSchema(report(Cluster.builder().withSSL()));
  }

  @Test(groups = "unit")
  public void should_conform_to_schema_for_socket_overrides() throws Exception {
    SocketOptions socketOptions =
        new SocketOptions()
            .setKeepAlive(true)
            .setReuseAddress(true)
            .setSoLinger(15)
            .setReceiveBufferSize(4096)
            .setSendBufferSize(8192);
    assertConformsToSchema(report(Cluster.builder().withSocketOptions(socketOptions)));
  }

  @Test(groups = "unit")
  public void should_reject_a_report_that_violates_the_schema() throws Exception {
    // Sanity check that the validator actually enforces the schema (rather than accepting
    // anything): an unknown top-level key must be rejected, since the schema sets
    // additionalProperties=false.
    ObjectNode report = (ObjectNode) report(Cluster.builder());
    report.put("bogus-unknown-key", "x");
    assertThat(violations(report))
        .as("unknown top-level key must be rejected")
        .contains(
            "$: property 'bogus-unknown-key' is not defined in the schema and the schema does not "
                + "allow additional properties");
  }

  // ---- Helpers --------------------------------------------------------------------------------

  /**
   * Drives {@code policy} through the initialization {@code Cluster.Manager.init()} performs after
   * the first control connection's handshake, with a single node in the given datacenter and rack,
   * so that the policy infers what it would infer against a live cluster. Stubbing the getters
   * would pin the reporter against a fiction; this exercises the policy's own inference.
   */
  private static void initWithNode(LoadBalancingPolicy policy, String datacenter, String rack) {
    Cluster cluster = mock(Cluster.class);
    when(cluster.getConfiguration()).thenReturn(Cluster.builder().getConfiguration());
    Host host = mock(Host.class);
    when(host.getDatacenter()).thenReturn(datacenter);
    when(host.getRack()).thenReturn(rack);
    policy.init(cluster, Collections.singletonList(host));
  }

  /** Builds the full report from a cluster builder and parses it. */
  private static JsonNode report(Cluster.Builder builder) throws IOException {
    String json = new DefaultDriverConfigReporter(builder.getConfiguration()).buildJson();
    return MAPPER.readTree(json);
  }

  // Accessors for the groups the schema nests, so that each test reads as what it asserts rather
  // than as a path walk. The full paths are pinned once, explicitly, in
  // should_report_default_configuration_shape.

  /** The {@code query.load-balancing} group. */
  private static JsonNode loadBalancing(JsonNode report) {
    return report.path("query").path("load-balancing");
  }

  /** The reported load balancing policy. */
  private static JsonNode lbPolicy(JsonNode report) {
    return loadBalancing(report).path("policy");
  }

  /** The reported node preference, which the schema nests next to the policy it derives from. */
  private static JsonNode nodePreference(JsonNode report) {
    return loadBalancing(report).path("node-preference");
  }

  /**
   * The schema's second node preference slot, under {@code connection}: the part of the same
   * preference that decides which hosts are pooled, i.e. the datacenter alone.
   */
  private static JsonNode connectionNodePreference(JsonNode report) {
    return report.path("connection").path("node-preference");
  }

  /** The {@code query.defaults} group. */
  private static JsonNode queryDefaults(JsonNode report) {
    return report.path("query").path("defaults");
  }

  /** The reported retry policy. */
  private static JsonNode retryPolicy(JsonNode report) {
    return report.path("query").path("retry").path("policy");
  }

  /** The reported speculative execution policy. */
  private static JsonNode speculativeExecution(JsonNode report) {
    return report.path("query").path("speculative-execution").path("policy");
  }

  /** A percentile speculative execution policy triggering at {@code percentile}. */
  private static PercentileSpeculativeExecutionPolicy percentile(double percentile) {
    return new PercentileSpeculativeExecutionPolicy(
        PerHostPercentileTracker.builder(1000).build(), percentile, 3);
  }

  /** The discriminator of the retry policy {@code builder} is configured with. */
  private static String retryPolicyType(Cluster.Builder builder) throws IOException {
    return retryPolicy(report(builder)).path("type").asText();
  }

  /** The {@code connection.socket} group. */
  private static JsonNode socket(JsonNode report) {
    return report.path("connection").path("socket");
  }

  private static Set<String> fieldNames(JsonNode node) {
    Set<String> names = new HashSet<String>();
    Iterator<String> iterator = node.fieldNames();
    while (iterator.hasNext()) {
      names.add(iterator.next());
    }
    return names;
  }

  /**
   * Asserts that {@code report} validates against the schema, save for any {@code extraGaps} the
   * caller's configuration is expected to add. Passing none therefore also pins that no
   * configuration silently grows a violation.
   */
  private static void assertConformsToSchema(JsonNode report, String... extraGaps) {
    Set<String> expected = new HashSet<String>(Arrays.asList(extraGaps));
    assertThat(violations(report)).as("schema violations in %s", report).isEqualTo(expected);
  }

  /**
   * The {@code load-distribution} reported for a token-aware policy ordering its replicas with
   * {@code replicaOrdering}.
   */
  private static String loadDistribution(TokenAwarePolicy.ReplicaOrdering replicaOrdering)
      throws IOException {
    JsonNode report =
        report(
            Cluster.builder()
                .withLoadBalancingPolicy(
                    new TokenAwarePolicy(
                        DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build(),
                        replicaOrdering)));
    assertConformsToSchema(report);
    return lbPolicy(report).path("load-distribution").asText();
  }

  private static Set<String> violations(JsonNode report) {
    Set<String> messages = new HashSet<String>();
    for (ValidationMessage message : SCHEMA.validate(report)) {
      messages.add(message.getMessage());
    }
    return messages;
  }

  /** A reporter that reports {@code json} verbatim, bypassing the configuration read. */
  private static DefaultDriverConfigReporter reporting(final String json) {
    return new DefaultDriverConfigReporter(config()) {
      @Override
      protected String buildJson() {
        return json;
      }
    };
  }

  /**
   * A report that is within the limit by {@link String#length()} but over it once encoded, so that
   * the check is pinned to UTF-8 bytes rather than characters.
   */
  private static String oversizedReport() {
    StringBuilder sb = new StringBuilder("{\"version\":1,\"pad\":\"");
    // 3 bytes each in UTF-8, so two thirds of the limit in characters is over it in bytes.
    for (int i = 0; i < (DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH * 2) / 3; i++) {
      sb.append('€');
    }
    String report = sb.append("\"}").toString();
    assertThat(report.length()).isLessThan(DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH);
    assertThat(report.getBytes(StandardCharsets.UTF_8).length)
        .isGreaterThan(DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH);
    return report;
  }

  /** A single-byte-per-character report of exactly {@code length} bytes. */
  private static String padTo(int length) {
    String prefix = "{\"version\":1,\"pad\":\"";
    String suffix = "\"}";
    StringBuilder sb = new StringBuilder(prefix);
    for (int i = prefix.length() + suffix.length(); i < length; i++) {
      sb.append('x');
    }
    String report = sb.append(suffix).toString();
    assertThat(report.getBytes(StandardCharsets.UTF_8).length).isEqualTo(length);
    return report;
  }

  /**
   * A user policy that matches none of the built-in types and is not chainable, so it is reported
   * as {@code custom}. Only its class name is ever read by the reporter.
   */
  private static class CustomLoadBalancingPolicy implements LoadBalancingPolicy {
    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {}

    @Override
    public HostDistance distance(Host host) {
      return HostDistance.LOCAL;
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
      return Collections.<Host>emptyList().iterator();
    }

    @Override
    public void onAdd(Host host) {}

    @Override
    public void onUp(Host host) {}

    @Override
    public void onDown(Host host) {}

    @Override
    public void onRemove(Host host) {}

    @Override
    public void close() {}
  }

  /** A speculative execution policy that is none of the built-ins, so reported by name only. */
  private static class CustomSpeculativeExecution implements SpeculativeExecutionPolicy {
    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
      throw new UnsupportedOperationException("never planned in this test");
    }

    @Override
    public void init(Cluster cluster) {}

    @Override
    public void close() {}
  }

  /**
   * A user timestamp generator that is neither of the driver's own, so whether it assigns a
   * timestamp client-side cannot be told from its class.
   */
  private static class CustomTimestamps implements TimestampGenerator {
    @Override
    public long next() {
      throw new UnsupportedOperationException("never called in this test");
    }
  }

  /**
   * A user reconnection policy that is neither of the built-ins, so it is reported as {@code
   * custom}. Only its class name is ever read by the reporter.
   */
  private static class CustomReconnectionPolicy implements ReconnectionPolicy {
    @Override
    public ReconnectionSchedule newSchedule() {
      throw new UnsupportedOperationException("never scheduled in this test");
    }

    @Override
    public void init(Cluster cluster) {}

    @Override
    public void close() {}
  }

  /** A chainable policy whose child is itself, i.e. a chain the reporter must not walk forever. */
  private static class CyclicLoadBalancingPolicy extends DelegatingLoadBalancingPolicy {
    CyclicLoadBalancingPolicy() {
      super(new RoundRobinPolicy());
    }

    @Override
    public LoadBalancingPolicy getChildPolicy() {
      return this;
    }
  }
}
