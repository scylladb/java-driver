/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.internal.core.loadbalancing.DseDcInferringLoadBalancingPolicy;
import com.datastax.dse.driver.internal.core.loadbalancing.DseLoadBalancingPolicy;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import com.datastax.oss.driver.internal.core.context.DriverConfigReporter.TlsInfo;
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.retry.ConsistencyDowngradingRetryPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.driver.internal.core.specex.ConstantSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.specex.NoSpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.time.AtomicTimestampGenerator;
import com.datastax.oss.driver.internal.core.time.ServerSideTimestampGenerator;
import com.datastax.oss.driver.internal.core.time.ThreadLocalTimestampGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.junit.Before;
import org.junit.Test;

// Many tests below use mock(SomeBuiltinPolicy.class) and assert the reporter recognizes it as that
// exact built-in (not "custom"). This relies on Mockito 5's default inline mock maker returning an
// object whose getClass() is the literal mocked class rather than a generated subclass (verified
// empirically for this project's Mockito version); a return to subclass-based mocking would make
// every exact-class branch under test here fall through to "custom" instead.
public class DefaultDriverConfigReporterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // The normative v1 JSON Schema from the design doc, shipped verbatim as a test resource. Loaded
  // once and pinned to draft 2020-12 (its declared $schema); its internal "#/$defs/..." refs
  // resolve locally, so validation needs no network access.
  private static final JsonSchema SCHEMA = loadSchema();

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

  // ---- Fixtures for the gating / fail-safe tests (bare mock profile) ----
  private InternalDriverContext mockContext;
  private DriverExecutionProfile mockProfile;
  private DefaultDriverConfigReporter reporter;

  @Before
  public void setup() {
    mockContext = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    mockProfile = mock(DriverExecutionProfile.class);
    when(mockContext.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(mockProfile);
    reporter = new DefaultDriverConfigReporter(mockContext);
  }

  private void enableReporting(boolean enabled) {
    when(mockProfile.getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, true))
        .thenReturn(enabled);
  }

  /** A reporter over the bare mock context whose report is fixed (or fails) as given. */
  private DefaultDriverConfigReporter reporterReporting(Supplier<String> json) {
    return new DefaultDriverConfigReporter(mockContext) {
      @Override
      String buildJson(TlsInfo tlsInfo) {
        return json.get();
      }
    };
  }

  // ==================== Gating ====================
  //
  // Note that SESSION_ID is not this class's concern: it is an innate startup option built by
  // StartupOptionsBuilder and sent on every connection regardless of these settings.

  @Test
  public void should_add_driver_config_when_enabled() {
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporterReporting(() -> "{\"version\":1}")
        .populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options)
        .hasSize(1)
        .containsEntry(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY, "{\"version\":1}");
  }

  @Test
  public void should_add_nothing_when_disabled() {
    enableReporting(false);
    Map<String, String> options = new HashMap<>();
    reporter.populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options).isEmpty();
  }

  @Test
  public void should_add_driver_config_when_the_option_is_not_defined() {
    // A configuration that omits the option altogether must behave like the shipped default, which
    // is enabled. Uses a real (map-based) profile: a mock would return false for any unstubbed
    // getBoolean(), ignoring the fallback that is under test here.
    Map<String, String> options = new HashMap<>();
    defaultsReporter(map -> map.remove(TypedDriverOption.DRIVER_CONFIG_REPORTING_ENABLED))
        .populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options).containsKey(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY);
  }

  @Test
  public void should_report_nothing_at_all_without_jackson() {
    // DefaultDriverContext substitutes NoopDriverConfigReporter when Jackson is absent, since
    // linking DefaultDriverConfigReporter on such a classpath raises a NoClassDefFoundError — an
    // Error, raised before any of its methods run, so neither its own fail-safe nor its caller in
    // ProtocolInitHandler could contain it. The absent-classpath path itself cannot be exercised
    // in-process; what is checked here is that the substitute contributes nothing and, in
    // particular, does not need a context to say so.
    Map<String, String> options = new HashMap<>();
    new NoopDriverConfigReporter().populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options).isEmpty();
  }

  // ==================== Fail-safe ====================

  @Test
  public void should_not_throw_when_reading_the_flag_fails() {
    when(mockProfile.getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, true))
        .thenThrow(new IllegalStateException("config blew up"));
    Map<String, String> options = new HashMap<>();
    reporter.populateControlConnectionOptions(options, TlsInfo.disabled()); // must not throw
    assertThat(options).isEmpty();
  }

  @Test
  public void should_skip_driver_config_when_building_fails() {
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporterReporting(
            () -> {
              throw new IllegalStateException("introspection blew up");
            })
        .populateControlConnectionOptions(options, TlsInfo.disabled()); // must not throw
    assertThat(options).isEmpty();
  }

  @Test
  public void should_skip_driver_config_when_serialization_fails() {
    // buildJson() returns null when Jackson fails to serialize the node tree.
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporterReporting(() -> null).populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options).isEmpty();
  }

  @Test
  public void should_fall_back_to_the_binary_name_when_getSimpleName_throws_an_error()
      throws Exception {
    // customPolicy() reads getSimpleName() off arbitrary user-supplied policy objects, which has a
    // documented JDK edge case throwing InternalError for certain synthetic classes. That is caught
    // where it happens rather than by the top-level catch, so the report is still produced -- with
    // the binary name, which is always available. No class a test can declare provokes the error,
    // so it is raised from the seam the production code guards.
    LoadBalancingPolicy custom = mock(LoadBalancingPolicy.class); // not a built-in
    DefaultDriverConfigReporter r =
        new DefaultDriverConfigReporter(
            contextWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                custom,
                clientSideGenerator(),
                Optional.empty(),
                Optional.empty(),
                null)) {
          @Override
          String simpleName(Class<?> policyClass) {
            throw new InternalError("simulated getSimpleName() JDK edge case");
          }
        };
    JsonNode lb = report(r).get("query").get("load-balancing").get("policy");
    assertThat(lb.get("type").asText()).isEqualTo("custom");
    assertThat(lb.get("name").asText()).isEqualTo(custom.getClass().getName());
  }

  @Test
  public void should_name_an_anonymous_policy_by_its_binary_name() throws Exception {
    // getSimpleName() is empty for an anonymous class -- a common way to supply a one-off policy --
    // and the schema's name is a nonEmptyString, so the binary name is used instead.
    SpeculativeExecutionPolicy anonymous =
        new SpeculativeExecutionPolicy() {
          @Override
          public long nextExecution(
              @NonNull Node node,
              @Nullable CqlIdentifier keyspace,
              @NonNull Request request,
              int runningExecutions) {
            return -1;
          }

          @Override
          public void close() {}
        };
    assertThat(anonymous.getClass().getSimpleName()).isEmpty();
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            anonymous,
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode specExec = report(r).get("query").get("speculative-execution").get("policy");
    assertThat(specExec.get("type").asText()).isEqualTo("custom");
    assertThat(specExec.get("name").asText()).isEqualTo(anonymous.getClass().getName());
  }

  @Test
  public void should_skip_driver_config_when_it_exceeds_the_size_limit() {
    // STARTUP option values are written with an unchecked 16-bit length prefix, so an oversized
    // report would corrupt the frame and fail the handshake rather than merely be useless. Parts of
    // the report come from unbounded user-supplied values (DC/rack names, consistency levels,
    // custom policy class names), so the limit has to be enforced here.
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporterReporting(() -> oversizedReport())
        .populateControlConnectionOptions(options, TlsInfo.disabled()); // must not throw
    assertThat(options).isEmpty();
  }

  @Test
  public void should_add_driver_config_that_is_just_within_the_size_limit() {
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    String atLimit = padTo(DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH);
    reporterReporting(() -> atLimit).populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options).containsEntry(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY, atLimit);
  }

  @Test
  public void should_skip_a_report_a_configuration_pushes_over_the_size_limit() throws Exception {
    // The two tests above stub buildJson(), so neither shows that a report can reach the limit at
    // all. This one drives the real serializer from a setting a user can make: the datacenter name
    // is one of the unbounded, user-supplied values the limit exists for, and it is emitted under
    // both node-preference parents, so half the limit in characters is enough to exceed it.
    DefaultDriverConfigReporter reporter =
        defaultsReporter(
            map ->
                map.put(
                    TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER,
                    repeat('d', DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH / 2 + 1)));

    // Built, well-formed and over the limit: it is dropped for its size, not because building it
    // failed. Reporting is left at the shipped default here, since defaultsReporter() reads a real
    // profile rather than the bare mock the tests above use.
    String json = reporter.buildJson(TlsInfo.disabled());
    assertConformsToSchema(MAPPER.readTree(json));
    assertThat(json.getBytes(StandardCharsets.UTF_8).length)
        .isGreaterThan(DefaultDriverConfigReporter.MAX_DRIVER_CONFIG_LENGTH);

    Map<String, String> options = new HashMap<>();
    reporter.populateControlConnectionOptions(options, TlsInfo.disabled());
    assertThat(options).isEmpty();
  }

  /** {@code String.repeat}, which core cannot use: it compiles against Java 8. */
  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
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

  // ==================== Report content ====================

  @Test
  public void should_report_default_configuration() throws Exception {
    JsonNode report = report(defaultsReporter(map -> {}));

    assertThat(report.get("version").asInt()).isEqualTo(DefaultDriverConfigReporter.SCHEMA_VERSION);

    // The whole document is three groups plus the version; everything else hangs off one of them.
    for (String group : new String[] {"connection", "control-plane", "query"}) {
      assertThat(report.has(group)).as("group %s present", group).isTrue();
    }
    // Nothing survives at the top level from the flat envelope of the previous schema revision.
    for (String group :
        new String[] {
          "socket",
          "reconnection-policy",
          "retry-policy",
          "load-balancing-policy",
          "speculative-execution-policy",
          "node-location-preference",
          "query-defaults",
          "connection-pool",
          "tls"
        }) {
      assertThat(report.has(group)).as("no top-level %s", group).isFalse();
    }
    // No speculative execution policy configured by default: the group has no null variant in
    // the schema, so it is omitted entirely rather than reported as null.
    assertThat(report.get("query").has("speculative-execution")).isFalse();

    JsonNode connection = report.get("connection");
    assertThat(connection.get("connect").get("timeout-ms").asLong()).isPositive();
    // No socket-level read/write timeout, and connection.heartbeat has no schema slot yet: all
    // three are omitted rather than present-with-null/empty.
    assertThat(connection.has("read")).isFalse();
    assertThat(connection.has("write")).isFalse();
    assertThat(connection.has("heartbeat")).isFalse();
    JsonNode requests = connection.get("requests");
    assertThat(requests.get("in-flight").get("max").asInt()).isEqualTo(1024);
    assertThat(requests.get("orphaned").get("max").asInt()).isEqualTo(256);
    assertThat(connection.get("pool").get("shard-aware").get("enabled").asBoolean()).isTrue();

    JsonNode socket = report.get("connection").get("socket");
    assertThat(socket.get("tcp-no-delay").asBoolean()).isTrue();
    assertThat(socket.get("keep-alive").asBoolean()).isFalse();
    assertThat(socket.has("linger")).isFalse();
    assertThat(socket.has("receive-buffer")).isFalse();
    assertThat(socket.has("send-buffer")).isFalse();

    JsonNode controlPlane = report.get("control-plane");
    assertThat(
            controlPlane.get("queries").get("system").get("timeout").get("client-side-ms").asLong())
        .isPositive();
    assertThat(
            controlPlane.get("queries").get("system").get("timeout").get("server-side-ms").asLong())
        .isPositive();
    assertThat(controlPlane.get("schema").get("agreement").get("timeout-ms").asLong()).isPositive();

    JsonNode reconnection = report.get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("type").asText()).isEqualTo("exponential");
    assertThat(reconnection.get("base-ms").asLong()).isPositive();
    assertThat(reconnection.get("max-ms").asLong()).isPositive();
    // Java's built-in reconnection policies are unbounded: max-attempts is omitted.
    assertThat(reconnection.has("max-attempts")).isFalse();

    JsonNode retryGroup = report.get("query").get("retry");
    // "backoff" is a sibling of "policy", not a field on it: no built-in Java retry policy inserts
    // a
    // delay between attempts, so the optional key is omitted from the group.
    assertThat(retryGroup.has("backoff")).isFalse();
    JsonNode retry = retryGroup.get("policy");
    assertThat(retry.get("type").asText()).isEqualTo("standard-error-aware");
    // The schema's max-retries reports a configured retry limit; Java's built-ins hardcode
    // per-error-type rules instead of taking a count from configuration, so it is omitted.
    assertThat(retry.has("max-retries")).isFalse();

    JsonNode lb = report.get("query").get("load-balancing").get("policy");
    // Every built-in policy routes to token replicas, so they all share the one built-in type; what
    // told them apart in the previous schema revision now lives in the fields below.
    assertThat(lb.get("type").asText()).isEqualTo("token-aware");
    // Replicas at the head of the query plan are always shuffled, with no option to disable it.
    assertThat(lb.get("load-distribution").asText()).isEqualTo("shuffle");
    assertThat(lb.get("fallback-to-non-preferred-nodes").asBoolean()).isFalse();
    // Slow-replica avoidance is on by default for DefaultLoadBalancingPolicy; "latency" is not
    // among the signals because the driver samples when responses arrive, not how long they took.
    JsonNode adaptiveOrdering = lb.get("adaptive-ordering");
    assertThat(adaptiveOrdering.get("signals"))
        .extracting(JsonNode::asText)
        .containsExactly("response-rate", "in-flight-requests", "recovery-state");
    // The schema dropped the boolean that used to carry this: presence is what says it is on.
    assertThat(adaptiveOrdering.has("enabled")).isFalse();
    // local-dc/local-rack are no longer reported here; see the node preferences below.
    assertThat(lb.has("local-dc")).isFalse();
    assertThat(lb.has("local-rack")).isFalse();

    // local-datacenter not configured in the defaults => DC is inferred (dc-auto). The policy is a
    // mock, so it has resolved nothing, which is also the real state when the very first control
    // connection sends STARTUP.
    JsonNode nodeLocation = report.get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("dc-auto");
    assertThat(nodeLocation.has("local-dc")).isFalse();
    assertThat(nodeLocation.has("inferred-local-dc")).isFalse();
    // The same preference reaches the connection group, which is scoped by the DC alone.
    JsonNode connectionNodeLocation = connection.get("node-preference");
    assertThat(connectionNodeLocation.get("type").asText()).isEqualTo("dc-auto");
    assertThat(connectionNodeLocation.has("local-dc")).isFalse();

    JsonNode query = report.get("query").get("defaults");
    assertThat(query.get("consistency").asText()).isEqualTo("LOCAL_ONE");
    // Unlike the schema's "absent when unset" wording suggests, Java always has a serial level
    // configured: basic.request.serial-consistency is a required option with a shipped default.
    assertThat(query.get("serial-consistency").asText()).isEqualTo("SERIAL");
    assertThat(query.get("idempotence").asBoolean()).isFalse();
    assertThat(query.get("client-timestamps").asBoolean()).isTrue();
    assertThat(query.get("request").get("timeout-ms").asLong()).isPositive();
    assertThat(query.get("page").get("size").asInt()).isPositive();

    // No SSL configured: the group is absent rather than an enabled:false, which the schema no
    // longer has a field for.
    assertThat(connection.has("tls")).isFalse();
  }

  @Test
  public void should_report_server_side_timeout_whatever_the_backend() throws Exception {
    // Configuration intent, not observed effect: metadata.schema.request-timeout goes on the wire
    // as a "USING TIMEOUT" clause only where CassandraSchemaQueries finds sharding information, but
    // the report describes what the driver is configured with, which it knows before it connects.
    JsonNode report = report(defaultsReporter(map -> {}));
    JsonNode timeout = report.get("control-plane").get("queries").get("system").get("timeout");
    assertThat(timeout.get("client-side-ms").asLong()).isPositive();
    assertThat(timeout.get("server-side-ms").asLong()).isPositive();
  }

  @Test
  public void should_report_constant_reconnection_policy() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            constantReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode reconnection = report(r).get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("type").asText()).isEqualTo("constant");
    assertThat(reconnection.get("delay-ms").asLong()).isPositive();
    assertThat(reconnection.has("max-attempts")).isFalse();
  }

  @Test
  public void should_report_custom_reconnection_policy() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            mock(ReconnectionPolicy.class), // neither exponential nor constant
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode reconnection = report(r).get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("type").asText()).isEqualTo("custom");
    assertThat(reconnection.get("name").asText()).isNotEmpty();
  }

  @Test
  public void should_report_a_reconnection_policy_subclass_as_custom() throws Exception {
    // A real (anonymous) subclass of a built-in, not a mock: proves the exact-class check doesn't
    // misclassify user customizations of a built-in as the plain built-in.
    // ConstantReconnectionPolicy is not final, and a real subclass of it already exists elsewhere
    // in this repo's test code.
    ReconnectionPolicy subclass = new ConstantReconnectionPolicy(policyConstructionContext()) {};
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            subclass,
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode reconnection = report(r).get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("type").asText()).isEqualTo("custom");
    // Also exercises the anonymous-class name fallback: getSimpleName() is empty for an anonymous
    // class, so the reported name must fall back to the (non-empty) binary class name.
    assertThat(reconnection.get("name").asText()).isNotEmpty();
  }

  @Test
  public void should_report_downgrading_consistency_retry_policy() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(ConsistencyDowngradingRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode retry = report(r).get("query").get("retry").get("policy");
    assertThat(retry.get("type").asText()).isEqualTo("downgrading-consistency");
    // No retry limit taken from configuration, so the schema's optional max-retries is omitted.
    assertThat(retry.has("max-retries")).isFalse();
  }

  @Test
  public void should_report_a_retry_policy_subclass_as_custom() throws Exception {
    // Real (anonymous) subclass, not a mock: DefaultRetryPolicy is not final, and a real subclass
    // of it already exists elsewhere in this repo's test code (osgi-tests' CustomRetryPolicy).
    RetryPolicy subclass = new DefaultRetryPolicy(policyConstructionContext(), "default") {};
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            subclass,
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode retry = report(r).get("query").get("retry").get("policy");
    assertThat(retry.get("type").asText()).isEqualTo("custom");
    assertThat(retry.get("name").asText()).isNotEmpty();
    // A custom policy cannot be introspected for a retry limit, so max-retries is omitted here too
    // even though the schema allows it on the custom variant.
    assertThat(retry.has("max-retries")).isFalse();
  }

  @Test
  public void should_report_constant_speculative_execution_policy() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            constantSpeculativeExecution(3, 100),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode spec = report(r).get("query").get("speculative-execution").get("policy");
    assertThat(spec.get("type").asText()).isEqualTo("constant");
    // The policy counts the initial, non-speculative execution among the 3 it caps, while the
    // schema field counts only the speculative ones — so 3 executions are 2 speculative.
    assertThat(spec.get("max-executions").asInt()).isEqualTo(2);
    assertThat(spec.get("delay-ms").asLong()).isEqualTo(100);
  }

  @Test
  public void should_report_speculative_executions_the_running_policy_was_built_with()
      throws Exception {
    // advanced.speculative-execution-policy is documented as not modifiable at runtime, and the
    // context builds the policy once: after a reload, the profile can carry values no request runs
    // with. The report describes the policy, so the reloaded 1 — which would omit the group as
    // "never speculates" — does not reach it.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(
                map -> {
                  map.put(TypedDriverOption.SPECULATIVE_EXECUTION_MAX, 1);
                  map.put(TypedDriverOption.SPECULATIVE_EXECUTION_DELAY, Duration.ofMillis(7));
                }),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            constantSpeculativeExecution(4, 100),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode spec = report.get("query").get("speculative-execution").get("policy");
    assertThat(spec.get("max-executions").asInt()).isEqualTo(3);
    assertThat(spec.get("delay-ms").asLong()).isEqualTo(100);
    assertConformsToSchema(report);
  }

  @Test
  public void should_omit_speculative_execution_policy_when_it_never_speculates() throws Exception {
    // max-executions = 1 is the smallest value ConstantSpeculativeExecutionPolicy accepts, and it
    // permits only the initial execution — so there is no speculative execution to describe, and
    // the
    // group is omitted exactly as it is for NoSpeculativeExecutionPolicy.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            constantSpeculativeExecution(1, 100),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    assertThat(report.get("query").has("speculative-execution")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_a_speculative_execution_policy_subclass_as_custom() throws Exception {
    // Real (anonymous) subclass, not a mock: NoSpeculativeExecutionPolicy is not final. A subclass
    // must be reported as "custom", not silently treated the same as "no policy" (which would drop
    // the whole group).
    SpeculativeExecutionPolicy subclass =
        new NoSpeculativeExecutionPolicy(policyConstructionContext(), "default") {};
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            subclass,
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode spec = report(r).get("query").get("speculative-execution").get("policy");
    assertThat(spec.get("type").asText()).isEqualTo("custom");
    assertThat(spec.get("name").asText()).isNotEmpty();
  }

  // The schema's built-in load-balancing variant is a single type, so these tests no longer
  // distinguish the built-ins by "type" — they pin that each one is still recognized as a built-in
  // rather than falling through to "custom", and that adaptive-ordering tells them apart.

  @Test
  public void should_report_dc_inferring_load_balancing_policy() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(
                DcInferringLoadBalancingPolicy.class), // extends DefaultLoadBalancingPolicy
            clientSideGenerator(),
            Optional.empty());
    assertBuiltInLoadBalancingPolicy(
        report(r).get("query").get("load-balancing").get("policy"), /* adaptiveOrdering= */ true);
  }

  @Test
  public void should_report_dse_load_balancing_policy_as_a_built_in() throws Exception {
    // DseLoadBalancingPolicy is a deprecated, behavior-identical alias of
    // DefaultLoadBalancingPolicy; must not fall through to "custom". Note: a real (non-mocked)
    // instance of this class requires a resolvable local DC (it uses MandatoryLocalDcHelper) and
    // would fail to construct with no DC configured, unlike DcInferringLoadBalancingPolicy above;
    // the mock here bypasses that constructor validation, same as
    // should_report_default_configuration.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DseLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    assertBuiltInLoadBalancingPolicy(
        report(r).get("query").get("load-balancing").get("policy"), /* adaptiveOrdering= */ true);
  }

  @Test
  public void should_report_dse_dc_inferring_load_balancing_policy_as_a_built_in()
      throws Exception {
    // DseDcInferringLoadBalancingPolicy is a deprecated, behavior-identical alias of
    // DcInferringLoadBalancingPolicy; must not fall through to "custom".
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DseDcInferringLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    assertBuiltInLoadBalancingPolicy(
        report(r).get("query").get("load-balancing").get("policy"), /* adaptiveOrdering= */ true);
  }

  @Test
  public void should_report_basic_load_balancing_policy_without_adaptive_ordering()
      throws Exception {
    // A real, distinct, documented third built-in (reference.conf lists exactly three); must not be
    // misclassified as "custom". Unlike DefaultLoadBalancingPolicy it has no slow-replica-avoidance
    // mechanism at all, so adaptive ordering is off regardless of the (default-policy-only) option
    // — which is the only thing now distinguishing it in the report.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true)),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(BasicLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    assertBuiltInLoadBalancingPolicy(
        report(r).get("query").get("load-balancing").get("policy"), /* adaptiveOrdering= */ false);
  }

  @Test
  public void should_report_adaptive_ordering_disabled_when_slow_avoidance_is_off()
      throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(
                DefaultLoadBalancingPolicy.class,
                /* avoidSlowReplicas= */ false,
                /* maxNodesPerRemoteDc= */ 0),
            clientSideGenerator(),
            Optional.empty());
    assertBuiltInLoadBalancingPolicy(
        report(r).get("query").get("load-balancing").get("policy"), /* adaptiveOrdering= */ false);
  }

  @Test
  public void should_report_the_adaptive_ordering_the_running_policy_was_built_with()
      throws Exception {
    // DefaultLoadBalancingPolicy latches slow-replica avoidance in its constructor and never
    // re-reads it, so after a reload the profile can say off while the policy keeps reordering.
    // The report describes the policy, so the reloaded false does not reach it.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, false)),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(
                DefaultLoadBalancingPolicy.class,
                /* avoidSlowReplicas= */ true,
                /* maxNodesPerRemoteDc= */ 0),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    assertBuiltInLoadBalancingPolicy(
        report.get("query").get("load-balancing").get("policy"), /* adaptiveOrdering= */ true);
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_custom_load_balancing_policy() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            mock(LoadBalancingPolicy.class), // not the default policy
            clientSideGenerator(),
            Optional.empty());
    JsonNode lb = report(r).get("query").get("load-balancing").get("policy");
    assertThat(lb.get("type").asText()).isEqualTo("custom");
    assertThat(lb.get("name").asText()).isNotEmpty();
  }

  @Test
  public void should_report_explicit_local_dc() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1"));
    JsonNode nodeLocation = report(r).get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("dc");
    assertThat(nodeLocation.get("local-dc").asText()).isEqualTo("dc1");
    assertThat(nodeLocation.has("local-rack")).isFalse();
  }

  @Test
  public void should_report_the_datacenter_half_of_the_preference_on_the_connection_group()
      throws Exception {
    // The two node-preference slots are not the same object. The datacenter scopes which nodes get
    // a pool at all (a node outside it is IGNORED, and an IGNORED node gets no connection), so it
    // belongs under "connection" — but the rack does not: it only reorders replicas at the head of
    // a query plan, and connections are still held across the whole local DC. Reporting the rack
    // there would claim a scoping the driver never performs.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
              map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1");
            });
    JsonNode report = report(r);

    JsonNode connectionPreference = report.get("connection").get("node-preference");
    assertThat(connectionPreference.get("type").asText()).isEqualTo("dc");
    assertThat(connectionPreference.get("local-dc").asText()).isEqualTo("dc1");
    assertThat(connectionPreference.has("local-rack")).isFalse();

    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("rack");
    assertThat(queryPreference.get("local-rack").asText()).isEqualTo("rack1");

    assertConformsToSchema(report);
  }

  @Test
  public void should_report_dc_auto_on_the_connection_group_when_only_a_rack_is_configured()
      throws Exception {
    // Rack-only: the query side keeps the rack (rack-auto, since the DC that goes with it is not
    // configured), while the connection side has no datacenter to name at all and degrades to a
    // bare dc-auto rather than borrowing the rack.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1"));
    JsonNode report = report(r);

    JsonNode connectionPreference = report.get("connection").get("node-preference");
    assertThat(connectionPreference.get("type").asText()).isEqualTo("dc-auto");
    assertThat(connectionPreference.has("local-dc")).isFalse();
    assertThat(connectionPreference.has("local-rack")).isFalse();

    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("rack-auto");
    assertThat(queryPreference.get("local-rack").asText()).isEqualTo("rack1");

    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_datacenter_the_policy_has_already_inferred() throws Exception {
    // On the very first control connection the policy has not been initialized and knows nothing,
    // so the report says dc-auto with no value. On every later reconnect it has resolved a real
    // local DC and uses it for routing — which is exactly what the schema's inferred fields are
    // for, and what an operator reading system.clients mid-incident needs to see.
    DefaultLoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn("dc-inferred");
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);

    // dc-auto carries the inferred datacenter in plain "local-dc" — the schema reserves the
    // "inferred-" prefix for rack-auto, where it has to be told apart from a configured one.
    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("dc-auto");
    assertThat(queryPreference.get("local-dc").asText()).isEqualTo("dc-inferred");

    JsonNode connectionPreference = report.get("connection").get("node-preference");
    assertThat(connectionPreference.get("type").asText()).isEqualTo("dc-auto");
    assertThat(connectionPreference.get("local-dc").asText()).isEqualTo("dc-inferred");

    assertConformsToSchema(report);
  }

  @Test
  public void should_report_an_inferred_datacenter_alongside_a_configured_rack() throws Exception {
    // A mixture: the rack was configured, the datacenter was worked out by the policy. rack-auto is
    // the variant for "at least one part is inferred", and it keeps the two apart by name.
    DefaultLoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn("dc-inferred");
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);

    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("rack-auto");
    assertThat(queryPreference.get("local-rack").asText()).isEqualTo("rack1");
    assertThat(queryPreference.get("inferred-local-dc").asText()).isEqualTo("dc-inferred");
    // The schema forbids carrying a configured and an inferred form of the same field.
    assertThat(queryPreference.has("local-dc")).isFalse();

    assertConformsToSchema(report);
  }

  @Test
  public void should_prefer_a_configured_datacenter_over_the_one_the_policy_inferred()
      throws Exception {
    // When the user configured the DC, the policy resolved to that same value — so it is reported
    // once, as configured. Emitting both forms would violate the schema and say nothing extra.
    DefaultLoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn("dc1");
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);

    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("dc");
    assertThat(queryPreference.get("local-dc").asText()).isEqualTo("dc1");
    assertThat(queryPreference.has("inferred-local-dc")).isFalse();

    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_datacenter_the_running_policy_resolved() throws Exception {
    // The context builds the load balancing policy once, so a profile reloaded to another
    // datacenter never reaches it: the policy keeps treating dc1 as local and an out-of-dc1 node
    // stays IGNORED. The report follows the policy, so the reloaded dc2 does not appear -- but the
    // value is still reported as configured, since that is where the policy took it from.
    DefaultLoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn("dc1");
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);

    for (JsonNode preference :
        new JsonNode[] {
          report.get("query").get("load-balancing").get("node-preference"),
          report.get("connection").get("node-preference")
        }) {
      assertThat(preference.get("type").asText()).isEqualTo("dc");
      assertThat(preference.get("local-dc").asText()).isEqualTo("dc1");
      assertThat(preference.has("inferred-local-dc")).isFalse();
    }
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_rack_the_running_policy_resolved() throws Exception {
    // Same for the rack half, which only query routing carries.
    DefaultLoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn("dc1");
    when(policy.getLocalRack()).thenReturn("rack1");
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(
                map -> {
                  map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
                  map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack2");
                }),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);

    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("rack");
    assertThat(queryPreference.get("local-dc").asText()).isEqualTo("dc1");
    assertThat(queryPreference.get("local-rack").asText()).isEqualTo("rack1");
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_configured_datacenter_before_the_policy_is_initialized()
      throws Exception {
    // The very first control connection sends STARTUP before the policy is initialized, so it has
    // resolved nothing yet and the configured value has to stand on its own -- the group must not
    // silently disappear on the one connection that carries the report.
    DefaultLoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn(null);
    when(policy.getLocalRack()).thenReturn(null);
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);

    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("dc");
    assertThat(queryPreference.get("local-dc").asText()).isEqualTo("dc1");
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_explicit_local_dc_and_rack() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
              map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1");
            });
    JsonNode nodeLocation = report(r).get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("rack");
    assertThat(nodeLocation.get("local-dc").asText()).isEqualTo("dc1");
    assertThat(nodeLocation.get("local-rack").asText()).isEqualTo("rack1");
  }

  @Test
  public void should_report_local_dc_set_via_session_builder() throws Exception {
    // SessionBuilder.withLocalDatacenter(...), not the config option: surfaced through
    // InternalDriverContext.getLocalDatacenter(), which the reporter must consult.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty(),
            /* programmaticLocalDc= */ "dc-programmatic");
    JsonNode nodeLocation = report(r).get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("dc");
    assertThat(nodeLocation.get("local-dc").asText()).isEqualTo("dc-programmatic");
  }

  @Test
  public void should_prefer_programmatic_local_dc_over_config_option() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(
                map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc-config")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty(),
            /* programmaticLocalDc= */ "dc-programmatic");
    JsonNode nodeLocation = report(r).get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("dc");
    assertThat(nodeLocation.get("local-dc").asText()).isEqualTo("dc-programmatic");
  }

  @Test
  public void should_report_rack_auto_when_only_rack_is_configured() throws Exception {
    // Rack configured explicitly, but no DC (neither programmatically nor via config): the DC will
    // be inferred, so this must not silently drop the explicitly-configured rack. local-rack is the
    // configured-value key, and the schema forbids pairing it with a configured local-dc here — so
    // the DC is absent under both its keys: there is no configured one, and the inferred one isn't
    // known at report time.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1"));
    JsonNode nodeLocation = report(r).get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("rack-auto");
    assertThat(nodeLocation.get("local-rack").asText()).isEqualTo("rack1");
    assertThat(nodeLocation.has("local-dc")).isFalse();
    assertThat(nodeLocation.has("inferred-local-dc")).isFalse();
    assertThat(nodeLocation.has("inferred-local-rack")).isFalse();
  }

  @Test
  public void should_report_dc_failover_when_it_is_configured_and_a_datacenter_is_preferred()
      throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(
                DefaultLoadBalancingPolicy.class,
                /* avoidSlowReplicas= */ true,
                /* maxNodesPerRemoteDc= */ 2),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    assertThat(
            report
                .get("query")
                .get("load-balancing")
                .get("policy")
                .get("fallback-to-non-preferred-nodes")
                .asBoolean())
        .isTrue();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_dc_failover_the_running_policy_was_built_with() throws Exception {
    // Same latching as adaptive ordering: BasicLoadBalancingPolicy reads max-nodes-per-remote-dc in
    // its constructor, so a profile reloaded to 0 leaves a policy that still appends remote nodes.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(
                map ->
                    map.put(
                        TypedDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC, 0)),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(
                DefaultLoadBalancingPolicy.class,
                /* avoidSlowReplicas= */ true,
                /* maxNodesPerRemoteDc= */ 2),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    assertThat(
            report
                .get("query")
                .get("load-balancing")
                .get("policy")
                .get("fallback-to-non-preferred-nodes")
                .asBoolean())
        .isTrue();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_dc_failover_as_off_without_a_datacenter_preference() throws Exception {
    // max-nodes-per-remote-dc is necessary but not sufficient: maybeAddDcFailover also needs the
    // policy to have a local DC, and BasicLoadBalancingPolicy with none configured never settles on
    // one. Reporting true off the option alone claimed failover for a session where no remote node
    // is ever appended to a query plan — and the key is about leaving the node preference, which
    // this report does not even carry.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(
                BasicLoadBalancingPolicy.class,
                /* avoidSlowReplicas= */ false,
                /* maxNodesPerRemoteDc= */ 2),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode loadBalancing = report.get("query").get("load-balancing");
    assertThat(loadBalancing.has("node-preference")).isFalse();
    assertThat(loadBalancing.get("policy").get("fallback-to-non-preferred-nodes").asBoolean())
        .isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_omit_node_location_preference_for_a_dc_agnostic_basic_policy()
      throws Exception {
    // BasicLoadBalancingPolicy is the one built-in that uses OptionalLocalDcHelper: with no DC
    // configured it stays datacenter-agnostic for the life of the session instead of inferring one,
    // so reporting dc-auto would describe a preference that never forms. The group is optional, so
    // the honest answer is to leave it out.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(BasicLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    assertThat(report(r).get("query").get("load-balancing").has("node-preference")).isFalse();
  }

  @Test
  public void should_omit_node_location_preference_for_a_basic_policy_with_only_a_rack()
      throws Exception {
    // Same case, and the rack does not rescue it: BasicLoadBalancingPolicy only looks for a rack
    // once it knows a DC, so a rack configured without one never takes effect either.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(BasicLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    assertThat(report(r).get("query").get("load-balancing").has("node-preference")).isFalse();
  }

  @Test
  public void should_report_a_configured_dc_for_a_basic_policy() throws Exception {
    // With a DC configured, BasicLoadBalancingPolicy does honor it, so the group is reported.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1")),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(BasicLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode nodeLocation = report(r).get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("dc");
    assertThat(nodeLocation.get("local-dc").asText()).isEqualTo("dc1");
  }

  @Test
  public void should_report_dc_auto_for_an_inferring_policy_without_a_local_dc() throws Exception {
    // The counterpart to the basic-policy cases above: DcInferringLoadBalancingPolicy really does
    // resolve a DC from the first contacted node, so dc-auto describes a preference it will form.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DcInferringLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    assertThat(
            report(r)
                .get("query")
                .get("load-balancing")
                .get("node-preference")
                .get("type")
                .asText())
        .isEqualTo("dc-auto");
  }

  @Test
  public void should_omit_node_location_preference_for_a_dc_agnostic_custom_policy()
      throws Exception {
    // The LoadBalancingPolicy SPI nowhere requires an implementation to infer a datacenter, so a
    // custom policy is in the same position as BasicLoadBalancingPolicy above, not the same as the
    // built-ins that do infer: dc-auto would be a claim the driver cannot make on its behalf.
    // Omitted from both parents, since the group is optional under each.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            mock(LoadBalancingPolicy.class), // not a built-in
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    assertThat(report.get("query").get("load-balancing").has("node-preference")).isFalse();
    assertThat(report.get("connection").has("node-preference")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_a_datacenter_a_non_inferring_policy_has_nonetheless_resolved()
      throws Exception {
    // The flip side: a subclass falls through to "custom" for the policy type, but if it has in
    // fact
    // settled on a local datacenter then that is evidence, not guesswork, and outranks the
    // exact-class rule that decided the case above.
    BasicLoadBalancingPolicy policy = loadBalancing(BasicLoadBalancingPolicy.class);
    when(policy.getLocalDatacenter()).thenReturn("dc-resolved");
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            policy,
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode queryPreference = report.get("query").get("load-balancing").get("node-preference");
    assertThat(queryPreference.get("type").asText()).isEqualTo("dc-auto");
    assertThat(queryPreference.get("local-dc").asText()).isEqualTo("dc-resolved");
    assertThat(report.get("connection").get("node-preference").get("local-dc").asText())
        .isEqualTo("dc-resolved");
    assertConformsToSchema(report);
  }

  @Test
  public void should_treat_a_blank_local_dc_and_rack_as_unset() throws Exception {
    // The driver's own helpers accept an empty local-datacenter as "set" (it just matches no node),
    // but the schema requires a non-empty string — and omitting the key while keeping type "dc"
    // would be invalid too. Treating blank as unset is the only schema-valid reading.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "  ");
              map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "");
            });
    JsonNode report = report(r);
    JsonNode nodeLocation = report.get("query").get("load-balancing").get("node-preference");
    assertThat(nodeLocation.get("type").asText()).isEqualTo("dc-auto");
    assertThat(nodeLocation.has("local-dc")).isFalse();
    assertThat(nodeLocation.has("local-rack")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_a_padded_local_dc_verbatim() throws Exception {
    // OptionalLocalDcHelper hands the configured string to the policy as is and matches it with
    // Objects.equals, so " dc1 " is a datacenter that matches no node. nonEmptyString admits it, so
    // it is reported as configured rather than normalized — trimming would hide the very typo an
    // operator reads this report to find.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, " dc1 "));
    JsonNode report = report(r);
    assertThat(
            report
                .get("query")
                .get("load-balancing")
                .get("node-preference")
                .get("local-dc")
                .asText())
        .isEqualTo(" dc1 ");
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_server_side_timestamps_as_disabled_client_timestamps()
      throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            mock(ServerSideTimestampGenerator.class),
            Optional.empty());
    assertThat(report(r).get("query").get("defaults").get("client-timestamps").asBoolean())
        .isFalse();
  }

  @Test
  public void should_omit_client_timestamps_for_an_unrecognized_generator() throws Exception {
    // A generator this class does not recognize may assign timestamps either way — the interface
    // lets it return Statement.NO_DEFAULT_TIMESTAMP from next() and leave them to the coordinator —
    // and naming the wrong source for every write the session makes is worse than saying nothing.
    // The schema's field is optional precisely so this document stays valid.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            mock(TimestampGenerator.class),
            Optional.empty());
    JsonNode report = report(r);
    assertThat(report.get("query").get("defaults").has("client-timestamps")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_every_built_in_generator() throws Exception {
    // None of the built-ins is ever reported as unknown: the two monotonic ones always assign the
    // timestamp themselves, and the server-side one never does. Real instances rather than mocks,
    // so that the branches are pinned to the classes the driver actually instantiates.
    assertThat(clientTimestampsOf(new AtomicTimestampGenerator(policyConstructionContext())))
        .isTrue();
    assertThat(clientTimestampsOf(new ThreadLocalTimestampGenerator(policyConstructionContext())))
        .isTrue();
    assertThat(clientTimestampsOf(new ServerSideTimestampGenerator(policyConstructionContext())))
        .isFalse();
  }

  /** The {@code query.defaults.client-timestamps} a report built over this generator carries. */
  private Boolean clientTimestampsOf(TimestampGenerator generator) throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            generator,
            Optional.empty());
    JsonNode clientTimestamps = report(r).get("query").get("defaults").get("client-timestamps");
    assertThat(clientTimestamps).isNotNull();
    return clientTimestamps.asBoolean();
  }

  @Test
  public void should_report_tls_enabled_with_hostname_verification() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode connection = report(r, TlsInfo.enabled(true)).get("connection");
    // Presence of the group is what reports TLS as on: the schema dropped the "enabled" boolean.
    assertThat(connection.has("tls")).isTrue();
    assertThat(connection.get("tls").get("hostname-verification").asBoolean()).isTrue();
  }

  @Test
  public void should_report_hostname_verification_from_active_tls_snapshot_not_config_option()
      throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.SSL_HOSTNAME_VALIDATION, true)),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode connection = report(r, TlsInfo.enabled(false)).get("connection");
    assertThat(connection.has("tls")).isTrue();
    assertThat(connection.get("tls").get("hostname-verification").asBoolean()).isFalse();
  }

  @Test
  public void should_omit_hostname_verification_when_active_engine_state_is_unknown()
      throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r, TlsInfo.enabledWithUnknownHostnameVerification());
    assertThat(report.get("connection").get("tls").has("hostname-verification")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_not_report_tls_when_the_configured_ssl_handler_was_removed() throws Exception {
    SslEngineFactory factory = new ProgrammaticSslEngineFactory(SSLContext.getDefault());
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.of(factory));

    assertThat(report(r).get("connection").has("tls")).isFalse();
  }

  @Test
  public void should_report_tls_when_a_pipeline_hook_added_an_ssl_handler() throws Exception {
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());

    JsonNode report = report(r, TlsInfo.enabled(false));
    JsonNode tls = report.get("connection").get("tls");
    assertThat(tls).isNotNull();
    assertThat(tls.get("hostname-verification").asBoolean()).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_active_hostname_verification_for_an_unrecognized_engine_factory()
      throws Exception {
    SslEngineFactory unrecognized = mock(SslEngineFactory.class);
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.of(unrecognized));
    JsonNode report = report(r, TlsInfo.enabled(true));
    JsonNode tls = report.get("connection").get("tls");
    assertThat(tls).isNotNull();
    assertThat(tls.get("hostname-verification").asBoolean()).isTrue();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_active_hostname_verification_not_the_configured_factory()
      throws Exception {
    SslEngineFactory configuredButUnused = mock(SslEngineFactory.class);
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.of(configuredButUnused),
            Optional.of(mock(SslHandlerFactory.class)),
            /* programmaticLocalDc= */ null);
    JsonNode connection = report(r, TlsInfo.enabled(false)).get("connection");
    assertThat(connection.get("tls").get("hostname-verification").asBoolean()).isFalse();
  }

  @Test
  public void should_not_resolve_the_configured_engine_factory_at_all() throws Exception {
    // Stronger than "reads the right one": the reporter must not touch getSslEngineFactory(). It is
    // a LazyReference nothing on this path has necessarily forced yet, and resolving the built-in
    // factory reads keystore/truststore files — on a Netty event-loop thread, mid-STARTUP, and
    // throwing there would cost the whole report.
    // Every mock is built before the stubbing below starts: the helpers stub their own returns, and
    // Mockito cannot have a when(...) open while another begins.
    ReconnectionPolicy reconnection = exponentialReconnection();
    TimestampGenerator timestamps = clientSideGenerator();
    // Built before the stubbing chain below: the helper stubs the policy itself, and Mockito
    // rejects a nested when() inside an unfinished one.
    LoadBalancingPolicy policy = loadBalancing(DefaultLoadBalancingPolicy.class);

    InternalDriverContext ctx = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    when(ctx.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaults(map -> {}));
    when(ctx.getReconnectionPolicy()).thenReturn(reconnection);
    when(ctx.getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(mock(DefaultRetryPolicy.class));
    when(ctx.getSpeculativeExecutionPolicy(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(mock(NoSpeculativeExecutionPolicy.class));
    when(ctx.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(policy);
    when(ctx.getTimestampGenerator()).thenReturn(timestamps);
    when(ctx.getSslHandlerFactory()).thenThrow(new AssertionError("must not be resolved"));
    when(ctx.getSslEngineFactory())
        .thenThrow(new AssertionError("the configured engine factory must not be resolved"));

    JsonNode report =
        MAPPER.readTree(new DefaultDriverConfigReporter(ctx).buildJson(TlsInfo.enabled(true)));
    assertThat(report.get("connection").get("tls").get("hostname-verification").asBoolean())
        .isTrue();
  }

  @Test
  public void should_not_report_hostname_verification_from_an_unused_engine_factory_as_enabled()
      throws Exception {
    SslEngineFactory validating =
        new ProgrammaticSslEngineFactory(
            SSLContext.getDefault(), null, /* requireHostnameValidation= */ true);
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.of(validating),
            Optional.of(mock(SslHandlerFactory.class)),
            /* programmaticLocalDc= */ null);
    JsonNode connection = report(r, TlsInfo.enabled(false)).get("connection");
    assertThat(connection.has("tls")).isTrue();
    assertThat(connection.get("tls").get("hostname-verification").asBoolean()).isFalse();
  }

  @Test
  public void should_report_socket_overrides() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.SOCKET_KEEP_ALIVE, true);
              map.put(TypedDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, 65535);
              map.put(TypedDriverOption.SOCKET_LINGER_INTERVAL, 5);
            });
    JsonNode socket = report(r).get("connection").get("socket");
    assertThat(socket.get("keep-alive").asBoolean()).isTrue();
    assertThat(socket.get("receive-buffer").get("size-bytes").asInt()).isEqualTo(65535);
    assertThat(socket.get("linger").get("interval-s").asInt()).isEqualTo(5);
  }

  // ==================== Values the schema cannot express ====================
  //
  // Options whose legal values can fall outside the schema's constraints — usually because
  // "disabled" is spelled 0, and twice because the driver does not enforce what the schema does.
  // Where the field is optional the group is omitted (as "page" already is when paging is
  // unbounded); where it is required, the real value is reported even though that document fails
  // validation — see the reporter's class javadoc.

  @Test
  public void should_omit_linger_when_disabled() throws Exception {
    // A negative interval means SO_LINGER is off, which the schema's non-negative interval-s
    // cannot express.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.SOCKET_LINGER_INTERVAL, -1));
    assertThat(report(r).get("connection").get("socket").has("linger")).isFalse();
  }

  @Test
  public void should_report_zero_linger_interval() throws Exception {
    // 0 is a real setting (close immediately), not a disabled sentinel, and the schema allows
    // it: it must survive the guard above.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.SOCKET_LINGER_INTERVAL, 0));
    assertThat(report(r).get("connection").get("socket").get("linger").get("interval-s").asInt())
        .isZero();
  }

  @Test
  public void should_omit_socket_buffers_when_not_positive() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, 0);
              map.put(TypedDriverOption.SOCKET_SEND_BUFFER_SIZE, 0);
            });
    JsonNode socket = report(r).get("connection").get("socket");
    assertThat(socket.has("receive-buffer")).isFalse();
    assertThat(socket.has("send-buffer")).isFalse();
  }

  @Test
  public void should_omit_client_side_timeout_when_control_connection_timeout_is_disabled()
      throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ZERO));
    JsonNode systemQueries = report(r).get("control-plane").get("queries").get("system");
    // The field is optional, but its enclosing "timeout" object is required, so it stays present.
    assertThat(systemQueries.has("timeout")).isTrue();
    assertThat(systemQueries.get("timeout").has("client-side-ms")).isFalse();
  }

  @Test
  public void should_report_one_millisecond_for_a_sub_millisecond_client_side_timeout()
      throws Exception {
    // AdminRequestHandler schedules this timeout in nanoseconds, so a sub-millisecond value is a
    // live timeout. Truncating it to 0 would land it on the very value both this option and the
    // schema read as "no timeout", so it floors at 1 instead.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map ->
                map.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofNanos(500_000)));
    assertThat(
            report(r)
                .get("control-plane")
                .get("queries")
                .get("system")
                .get("timeout")
                .get("client-side-ms")
                .asLong())
        .isEqualTo(1);
  }

  @Test
  public void should_report_one_millisecond_for_a_sub_millisecond_schema_agreement_timeout()
      throws Exception {
    // SchemaAgreementChecker holds this timeout in nanoseconds and only skips the check outright at
    // 0, which is also what the schema documents 0 to mean ("do not wait") — so a positive
    // sub-millisecond timeout, which does wait, must not collapse onto it.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map ->
                map.put(
                    TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT,
                    Duration.ofNanos(500_000)));
    assertThat(
            report(r)
                .get("control-plane")
                .get("schema")
                .get("agreement")
                .get("timeout-ms")
                .asLong())
        .isEqualTo(1);
  }

  @Test
  public void should_report_one_millisecond_for_a_sub_millisecond_request_timeout()
      throws Exception {
    // Same reasoning: CqlRequestHandler schedules the request timeout in nanoseconds, and 0 is how
    // this option spells "disabled".
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofNanos(500_000)));
    assertThat(report(r).get("query").get("defaults").get("request").get("timeout-ms").asLong())
        .isEqualTo(1);
  }

  @Test
  public void should_omit_a_sub_millisecond_connect_timeout() throws Exception {
    // Deliberately not floored at 1, unlike the timeouts above: DefaultNettyOptions hands the
    // truncated millisecond value to Netty's CONNECT_TIMEOUT_MILLIS, where 0 disables the timeout,
    // so a sub-millisecond connect timeout genuinely is disabled at runtime. The enclosing
    // "connect" object is required and stays present; only the key goes.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map ->
                map.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofNanos(500_000)));
    JsonNode connection = report(r).get("connection");
    assertThat(connection.has("connect")).isTrue();
    assertThat(connection.get("connect").has("timeout-ms")).isFalse();
  }

  @Test
  public void should_omit_a_disabled_connect_timeout() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ZERO));
    JsonNode report = report(r);
    assertThat(report.get("connection").get("connect").has("timeout-ms")).isFalse();
    // Unlike the request timeout, this one has a schema-valid representation now that the key is
    // optional, so the document stays valid.
    assertConformsToSchema(report);
  }

  @Test
  public void should_omit_a_sub_millisecond_server_side_timeout() throws Exception {
    // Also deliberately not floored: this value goes on the wire as the millisecond argument of a
    // USING TIMEOUT clause, so sub-millisecond really is 0ms server-side.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map ->
                map.put(
                    TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofNanos(500_000)));
    assertThat(
            report(r)
                .get("control-plane")
                .get("queries")
                .get("system")
                .get("timeout")
                .has("server-side-ms"))
        .isFalse();
  }

  @Test
  public void should_omit_server_side_timeout_when_schema_request_timeout_is_disabled()
      throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ZERO));
    assertThat(
            report(r)
                .get("control-plane")
                .get("queries")
                .get("system")
                .get("timeout")
                .has("server-side-ms"))
        .isFalse();
  }

  @Test
  public void should_clamp_negative_schema_agreement_timeout_to_zero() throws Exception {
    // Required and non-negative in the schema. A negative timeout behaves exactly like 0 (the first
    // pass is already past the deadline), so normalizing is exact rather than invented.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map ->
                map.put(
                    TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT,
                    Duration.ofSeconds(-1)));
    assertThat(
            report(r)
                .get("control-plane")
                .get("schema")
                .get("agreement")
                .get("timeout-ms")
                .asLong())
        .isZero();
  }

  @Test
  public void should_report_configured_request_capacity() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.CONNECTION_MAX_REQUESTS, 2048);
              map.put(TypedDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, 512);
            });
    JsonNode requests = report(r).get("connection").get("requests");
    assertThat(requests.get("in-flight").get("max").asInt()).isEqualTo(2048);
    assertThat(requests.get("orphaned").get("max").asInt()).isEqualTo(512);
  }

  @Test
  public void should_report_the_corrected_orphaned_request_threshold() throws Exception {
    // ChannelFactory requires max-orphan-requests to stay below max-requests-per-connection and
    // silently substitutes a quarter of the latter when it doesn't. Reporting the configured 1024
    // here would describe a threshold no connection was ever built with.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.CONNECTION_MAX_REQUESTS, 1024);
              map.put(TypedDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, 1024);
            });
    JsonNode report = report(r);
    assertThat(report.get("connection").get("requests").get("orphaned").get("max").asInt())
        .isEqualTo(256);
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_maximum_supported_in_flight_request_count() throws Exception {
    // The largest value reference.conf documents for this option ("less than 32768"). The schema no
    // longer caps the field at all, so this and anything above it are valid documents; the only
    // in-flight value it rejects is a non-positive one, pinned below.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.CONNECTION_MAX_REQUESTS, 32767));
    JsonNode report = report(r);
    assertThat(report.get("connection").get("requests").get("in-flight").get("max").asInt())
        .isEqualTo(32767);
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_an_in_flight_max_above_the_documented_bound() throws Exception {
    // reference.conf documents "strictly positive, and less than 32768", but nothing enforces the
    // upper half of that and the schema no longer encodes it either, so a value past it is simply
    // reported — and now validates, where it used to be a knowingly invalid document.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.CONNECTION_MAX_REQUESTS, 40000));
    JsonNode report = report(r);
    assertThat(report.get("connection").get("requests").get("in-flight").get("max").asInt())
        .isEqualTo(40000);
    assertConformsToSchema(report);
  }

  @Test
  public void should_omit_the_request_group_when_the_request_timeout_is_disabled()
      throws Exception {
    // basic.request.timeout = 0 legally disables the request timeout, and timeout-ms is
    // positive-only — but both it and its enclosing "request" object are optional, so "disabled" is
    // said by omission and the document stays valid. This used to be one of the schema gaps the
    // reporter had to knowingly violate; making "request" optional closed it.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ZERO));
    JsonNode report = report(r);
    assertThat(report.get("query").get("defaults").has("request")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_a_non_positive_in_flight_max_even_though_the_schema_forbids_it()
      throws Exception {
    // The schema requires a positive integer and nothing in the driver validates the option against
    // that: ChannelFactory hands the configured value straight to StreamIdGenerator, which does not
    // range-check it. Unreachable through a live session all the same — the connection fails first
    // (a negative value throws out of StreamIdGenerator's BitSet while the channel is being built;
    // zero leaves no stream id for the control connection's own OPTIONS) — so this pins the
    // behavior at the seam rather than describing a live exposure: the reporter describes what is
    // configured rather than substituting a limit no connection was built with.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.CONNECTION_MAX_REQUESTS, 0));
    JsonNode report = report(r);
    assertThat(report.get("connection").get("requests").get("in-flight").get("max").asInt())
        .isZero();
    assertThat(SCHEMA.validate(report))
        .as("the v1 schema requires a positive in-flight.max")
        .isNotEmpty();
  }

  @Test
  public void should_report_an_out_of_enum_consistency_even_though_the_schema_forbids_it()
      throws Exception {
    // The one knowingly invalid document a live session can actually produce, unlike its in-flight
    // sibling above, and the same trade-off. basic.request.consistency is an
    // unvalidated string while the schema's field is a closed enum, so a name outside it has no
    // schema-valid form and the field is required, leaving no omission route. Reaching this takes a
    // custom ConsistencyLevelRegistry that defines extra names (or a custom load balancing policy):
    // the built-in policies resolve this option through the registry in their constructor, so an
    // unknown name fails the session before any report is built. Reporting the effective
    // configuration means passing it through rather than aliasing it to something the operator did
    // not set, or dropping the whole report over one field.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.REQUEST_CONSISTENCY, "SITE_QUORUM"));
    JsonNode report = report(r);
    assertThat(report.get("query").get("defaults").get("consistency").asText())
        .isEqualTo("SITE_QUORUM");
    assertThat(SCHEMA.validate(report))
        .as("the v1 schema's consistency enum has no member for a custom level")
        .isNotEmpty();
  }

  @Test
  public void should_report_the_serial_levels_as_a_default_consistency() throws Exception {
    // basic.request.consistency accepts a serial level — a misconfiguration the server rejects for
    // regular reads and writes, but one the driver itself never refuses — and the schema's enum now
    // has members for both, so this is a valid document rather than the knowing violation it was.
    for (String level : new String[] {"SERIAL", "LOCAL_SERIAL"}) {
      DefaultDriverConfigReporter r =
          defaultsReporter(map -> map.put(TypedDriverOption.REQUEST_CONSISTENCY, level));
      JsonNode report = report(r);
      assertThat(report.get("query").get("defaults").get("consistency").asText()).isEqualTo(level);
      assertConformsToSchema(report);
    }
  }

  @Test
  public void should_omit_a_serial_consistency_the_schema_has_no_member_for() throws Exception {
    // The mirror image of the case above, and the reason it is not a second known gap:
    // basic.request.serial-consistency is just as unvalidated (nothing rejects a non-serial level
    // before the first conditional statement runs), but the schema's field is *optional*, so the
    // value can be said by omission instead of breaking the whole document.
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.REQUEST_SERIAL_CONSISTENCY, "QUORUM"));
    JsonNode report = report(r);
    assertThat(report.get("query").get("defaults").has("serial-consistency")).isFalse();
    assertConformsToSchema(report);
  }

  // ==================== Options a config source may not define ====================
  //
  // reference.conf and OptionsMap.driverDefaults() both cover every option this reporter reads, but
  // a custom DriverConfigLoader need not, and the no-fallback getters throw on a missing option. No
  // read may cost more than the field it describes: an optional field is omitted, a required one
  // falls back to the value reference.conf documents.

  @Test
  public void should_omit_optional_fields_whose_options_are_undefined() throws Exception {
    // Each of these maps to a schema field or group that is optional, so an undefined option is
    // reported the same way a disabled one is: by omission, leaving a valid document.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.remove(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT);
              map.remove(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT);
              map.remove(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);
              map.remove(TypedDriverOption.REQUEST_PAGE_SIZE);
              map.remove(TypedDriverOption.REQUEST_TIMEOUT);
            });
    JsonNode report = report(r);
    assertThat(report.get("connection").get("connect").has("timeout-ms")).isFalse();
    JsonNode systemTimeout =
        report.get("control-plane").get("queries").get("system").get("timeout");
    assertThat(systemTimeout.has("client-side-ms")).isFalse();
    assertThat(systemTimeout.has("server-side-ms")).isFalse();
    JsonNode defaults = report.get("query").get("defaults");
    assertThat(defaults.has("page")).isFalse();
    assertThat(defaults.has("request")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_the_speculative_execution_group_when_its_options_are_undefined()
      throws Exception {
    // max-executions and delay-ms are both required within the constant variant, and neither is
    // read from the profile: they come off the policy, which could not have been built at all had
    // the options been undefined then. So an option missing now costs the group nothing.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.remove(TypedDriverOption.SPECULATIVE_EXECUTION_MAX)),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            constantSpeculativeExecution(3, 100),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode specExec = report.get("query").get("speculative-execution").get("policy");
    assertThat(specExec.get("max-executions").asInt()).isEqualTo(2);
    assertThat(specExec.get("delay-ms").asLong()).isEqualTo(100);
    assertConformsToSchema(report);
  }

  @Test
  public void should_still_report_when_a_required_fields_option_is_undefined() throws Exception {
    // These five map to required schema fields, where omission would invalidate the whole document,
    // so each falls back to the value reference.conf documents. Removing all five at once is the
    // worst case: the report is still built, still complete, and still valid.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.remove(TypedDriverOption.CONNECTION_MAX_REQUESTS);
              map.remove(TypedDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS);
              map.remove(TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT);
              map.remove(TypedDriverOption.REQUEST_CONSISTENCY);
              map.remove(TypedDriverOption.REQUEST_DEFAULT_IDEMPOTENCE);
            });
    JsonNode report = report(r);
    JsonNode requests = report.get("connection").get("requests");
    assertThat(requests.get("in-flight").get("max").asInt()).isEqualTo(1024);
    assertThat(requests.get("orphaned").get("max").asInt()).isEqualTo(256);
    assertThat(
            report.get("control-plane").get("schema").get("agreement").get("timeout-ms").asLong())
        .isEqualTo(10_000);
    JsonNode defaults = report.get("query").get("defaults");
    assertThat(defaults.get("consistency").asText()).isEqualTo("LOCAL_ONE");
    assertThat(defaults.get("idempotence").asBoolean()).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_both_serial_consistency_levels() throws Exception {
    // Both members of the schema's enum survive the guard above; SERIAL is the shipped default and
    // is asserted by the default-report test, so this pins the other one.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.REQUEST_SERIAL_CONSISTENCY, "LOCAL_SERIAL"));
    JsonNode report = report(r);
    assertThat(report.get("query").get("defaults").get("serial-consistency").asText())
        .isEqualTo("LOCAL_SERIAL");
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_reconnection_delays_from_the_running_policy_not_the_profile()
      throws Exception {
    // Both built-in reconnection policies latch their delays into final fields at construction and
    // never re-read them, so after a configuration reload the profile describes delays that nothing
    // is reconnecting with. The instance is the only accurate source; the profile here deliberately
    // carries different numbers, standing in for a reload the running policy has not seen.
    ExponentialReconnectionPolicy policy = mock(ExponentialReconnectionPolicy.class);
    when(policy.getBaseDelayMs()).thenReturn(2000L);
    when(policy.getMaxDelayMs()).thenReturn(90000L);
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(
                map -> {
                  map.put(TypedDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(7));
                  map.put(TypedDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(11));
                }),
            policy,
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode reconnection = report.get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("base-ms").asLong()).isEqualTo(2000L);
    assertThat(reconnection.get("max-ms").asLong()).isEqualTo(90000L);
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_a_zero_constant_reconnection_delay() throws Exception {
    // ConstantReconnectionPolicy rejects only a negative base delay, so 0 is a legal setting
    // (reconnect immediately, no backoff) — unlike ExponentialReconnectionPolicy, which requires a
    // strictly positive base. The schema admits it too ("0 means reconnect immediately"), so this
    // is
    // reported verbatim and the document stays valid.
    //
    // The delay is stubbed on the policy, not just set in the profile, because the reporter reads
    // the running instance — see exponentialReconnection().
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> map.put(TypedDriverOption.RECONNECTION_BASE_DELAY, Duration.ZERO)),
            constantReconnection(Duration.ZERO),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode reconnection = report.get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("type").asText()).isEqualTo("constant");
    assertThat(reconnection.get("delay-ms").asLong()).isZero();
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_one_millisecond_for_a_sub_millisecond_constant_reconnection_delay()
      throws Exception {
    // The counterpart to the zero case above: ConstantReconnectionPolicy also accepts a positive
    // sub-millisecond delay, and Reconnection schedules nextDelay() in nanoseconds, so that delay
    // really does elapse between attempts. Truncating it would report the one value the schema
    // defines as "reconnect immediately" for a policy that does back off.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(
                map ->
                    map.put(TypedDriverOption.RECONNECTION_BASE_DELAY, Duration.ofNanos(500_000))),
            constantReconnection(Duration.ofNanos(500_000)),
            mock(DefaultRetryPolicy.class),
            mock(NoSpeculativeExecutionPolicy.class),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode reconnection = report.get("connection").get("reconnection").get("policy");
    assertThat(reconnection.get("type").asText()).isEqualTo("constant");
    assertThat(reconnection.get("delay-ms").asLong()).isEqualTo(1L);
    assertConformsToSchema(report);
  }

  @Test
  public void should_report_a_zero_speculative_execution_delay() throws Exception {
    // ConstantSpeculativeExecutionPolicy explicitly allows a zero delay ("Delay must be positive or
    // 0"), meaning every speculative execution fires at once, and the schema admits it ("0 means
    // launch immediately"). reference.conf also documents sub-millisecond delays as equivalent to
    // 0,
    // which is why this field is not floored at 1 the way the timeouts are — the policy has already
    // truncated them to the 0 it schedules with.
    DefaultDriverConfigReporter r =
        reporterWith(
            defaults(map -> {}),
            exponentialReconnection(),
            mock(DefaultRetryPolicy.class),
            constantSpeculativeExecution(3, 0),
            loadBalancing(DefaultLoadBalancingPolicy.class),
            clientSideGenerator(),
            Optional.empty());
    JsonNode report = report(r);
    JsonNode specExec = report.get("query").get("speculative-execution").get("policy");
    assertThat(specExec.get("type").asText()).isEqualTo("constant");
    assertThat(specExec.get("delay-ms").asLong()).isZero();
    assertConformsToSchema(report);
  }

  @Test
  public void should_omit_page_when_unbounded() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.REQUEST_PAGE_SIZE, 0));
    // The schema has no "unbounded" sentinel: the whole page group is omitted instead.
    assertThat(report(r).get("query").get("defaults").has("page")).isFalse();
  }

  @Test
  public void should_report_bounded_page_size() throws Exception {
    DefaultDriverConfigReporter r =
        defaultsReporter(map -> map.put(TypedDriverOption.REQUEST_PAGE_SIZE, 5000));
    assertThat(report(r).get("query").get("defaults").get("page").get("size").asInt())
        .isEqualTo(5000);
  }

  @Test
  public void should_report_shard_awareness_under_connection_pool() throws Exception {
    // The schema keeps no pool size or keying, so what is left of pooling is the shard-awareness
    // intent — configuration, not an observed effect: the option describes how a connection reaches
    // a chosen shard, not what the server turns out to be.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> map.put(TypedDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, false));
    JsonNode pool = report(r).get("connection").get("pool");
    assertThat(pool.get("shard-aware").get("enabled").asBoolean()).isFalse();
  }

  @Test
  public void should_not_report_a_pool_size_the_schema_no_longer_carries() throws Exception {
    // CONNECTION_POOL_LOCAL_SIZE has no schema slot any more, which is also what settles the
    // local-versus-remote question: neither size is reported, so neither can misstate the other.
    DefaultDriverConfigReporter r =
        defaultsReporter(
            map -> {
              map.put(TypedDriverOption.CONNECTION_POOL_LOCAL_SIZE, 8);
              map.put(TypedDriverOption.CONNECTION_POOL_REMOTE_SIZE, 3);
            });
    JsonNode report = report(r);
    assertThat(report.get("connection").get("pool").fieldNames())
        .toIterable()
        .containsExactly("shard-aware");
    assertConformsToSchema(report);
  }

  // ==================== Schema conformance ====================
  //
  // These build a config, serialize it via the reporter, and validate the produced JSON against the
  // normative v1 JSON Schema (the same document ScyllaDB uses to interpret DRIVER_CONFIG). They
  // cover every discriminated-union branch and optional-group case the reporter can emit, so that
  // schema conformance is enforced rather than assumed.
  //
  // Conformance is not unconditional: where a required schema field is positive-only and its driver
  // option legitimately admits 0, the reporter emits the real value and the document does not
  // validate. Those cases are deliberate and pinned in the "Values the schema cannot express"
  // section above, each asserting the violation explicitly — see the reporter's class javadoc.

  @Test
  public void should_conform_to_schema_for_default_report() throws Exception {
    assertConformsToSchema(report(defaultsReporter(map -> {})));
  }

  @Test
  public void should_conform_to_schema_for_constant_reconnection_policy() throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                constantReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_custom_reconnection_policy() throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                mock(ReconnectionPolicy.class),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_downgrading_consistency_retry_policy() throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(ConsistencyDowngradingRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_custom_retry_policy() throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(RetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_constant_speculative_execution_policy()
      throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                constantSpeculativeExecution(3, 100),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_custom_speculative_execution_policy() throws Exception {
    // The custom variant of the one discriminated union whose branches were otherwise only pinned
    // for the built-in: a policy that is neither of the two built-ins keeps the enclosing group
    // (unlike NoSpeculativeExecutionPolicy, which drops it) and carries just type + name.
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(SpeculativeExecutionPolicy.class),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_basic_load_balancing_policy() throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(BasicLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_custom_load_balancing_policy() throws Exception {
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                mock(LoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty())));
  }

  @Test
  public void should_conform_to_schema_for_explicit_dc_and_rack() throws Exception {
    assertConformsToSchema(
        report(
            defaultsReporter(
                map -> {
                  map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
                  map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1");
                })));
  }

  @Test
  public void should_conform_to_schema_for_rack_auto_node_location() throws Exception {
    assertConformsToSchema(
        report(
            defaultsReporter(
                map -> map.put(TypedDriverOption.LOAD_BALANCING_LOCAL_RACK, "rack1"))));
  }

  @Test
  public void should_conform_to_schema_for_tls_enabled_with_hostname_verification()
      throws Exception {
    SslEngineFactory factory =
        new ProgrammaticSslEngineFactory(
            SSLContext.getDefault(), null, /* requireHostnameValidation= */ true);
    assertConformsToSchema(
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(DefaultLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.of(factory))));
  }

  @Test
  public void should_conform_to_schema_for_socket_overrides() throws Exception {
    assertConformsToSchema(
        report(
            defaultsReporter(
                map -> {
                  map.put(TypedDriverOption.SOCKET_KEEP_ALIVE, true);
                  map.put(TypedDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, 65535);
                  map.put(TypedDriverOption.SOCKET_SEND_BUFFER_SIZE, 65535);
                  map.put(TypedDriverOption.SOCKET_LINGER_INTERVAL, 5);
                })));
  }

  @Test
  public void should_conform_to_schema_without_a_node_location_preference() throws Exception {
    // The datacenter-agnostic basic policy omits the group entirely; it is optional, so the
    // document
    // stays valid without it.
    JsonNode report =
        report(
            reporterWith(
                defaults(map -> {}),
                exponentialReconnection(),
                mock(DefaultRetryPolicy.class),
                mock(NoSpeculativeExecutionPolicy.class),
                loadBalancing(BasicLoadBalancingPolicy.class),
                clientSideGenerator(),
                Optional.empty()));
    assertThat(report.get("query").get("load-balancing").has("node-preference")).isFalse();
    assertConformsToSchema(report);
  }

  @Test
  public void should_conform_to_schema_when_all_optional_timeouts_are_disabled() throws Exception {
    // The case the omission guards exist for: every control-plane timeout that can be turned off
    // is, leaving system-queries.timeout an empty object — which validates, since that object has
    // no required keys.
    assertConformsToSchema(
        report(
            defaultsReporter(
                map -> {
                  map.put(TypedDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ZERO);
                  map.put(TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ZERO);
                  map.put(TypedDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ZERO);
                })));
  }

  @Test
  public void should_conform_to_schema_when_optional_socket_and_page_values_are_disabled()
      throws Exception {
    assertConformsToSchema(
        report(
            defaultsReporter(
                map -> {
                  map.put(TypedDriverOption.SOCKET_LINGER_INTERVAL, -1);
                  map.put(TypedDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, 0);
                  map.put(TypedDriverOption.SOCKET_SEND_BUFFER_SIZE, 0);
                  map.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ZERO);
                  map.put(TypedDriverOption.REQUEST_PAGE_SIZE, 0);
                })));
  }

  @Test
  public void should_reject_a_report_that_violates_the_schema() throws Exception {
    // Sanity check that the validator actually enforces the schema (rather than accepting
    // anything): an unknown top-level key must be rejected, since the schema sets
    // additionalProperties to false.
    ObjectNode report = (ObjectNode) report(defaultsReporter(map -> {}));
    report.put("bogus-unknown-key", "x");
    assertThat(SCHEMA.validate(report)).as("unknown top-level key must be rejected").isNotEmpty();
  }

  // ==================== helpers ====================

  private void assertConformsToSchema(JsonNode report) {
    Set<ValidationMessage> errors = SCHEMA.validate(report);
    assertThat(errors).as("schema violations in %s", report).isEmpty();
  }

  /**
   * Asserts the shape every built-in load balancing policy shares, given whether it reorders
   * candidates from runtime observations.
   */
  private void assertBuiltInLoadBalancingPolicy(JsonNode lb, boolean adaptiveOrdering) {
    assertThat(lb.get("type").asText()).isEqualTo("token-aware");
    assertThat(lb.get("load-distribution").asText()).isEqualTo("shuffle");
    // The schema has no "enabled" flag and requires a non-empty signal list, so adaptive ordering
    // is reported by the group's presence: absent when off, never present-but-empty.
    if (adaptiveOrdering) {
      assertThat(lb.get("adaptive-ordering").get("signals"))
          .extracting(JsonNode::asText)
          .containsExactly("response-rate", "in-flight-requests", "recovery-state");
    } else {
      assertThat(lb.has("adaptive-ordering")).isFalse();
    }
  }

  /**
   * An exponential reconnection policy with its delays stubbed.
   *
   * <p>The reporter reads these off the instance rather than the profile, because both built-ins
   * latch their delays at construction and a reload does not reach the running policy. A bare mock
   * would answer 0 for both, which the schema rejects — and would not exercise the accessor path at
   * all.
   */
  private static ExponentialReconnectionPolicy exponentialReconnection() {
    ExponentialReconnectionPolicy policy = mock(ExponentialReconnectionPolicy.class);
    when(policy.getBaseDelayMs()).thenReturn(1000L);
    when(policy.getMaxDelayMs()).thenReturn(60000L);
    return policy;
  }

  /**
   * A constant reconnection policy with its delay stubbed; see {@link #exponentialReconnection}.
   */
  private static ConstantReconnectionPolicy constantReconnection() {
    return constantReconnection(Duration.ofSeconds(1));
  }

  private static ConstantReconnectionPolicy constantReconnection(Duration delay) {
    ConstantReconnectionPolicy policy = mock(ConstantReconnectionPolicy.class);
    when(policy.getDelay()).thenReturn(delay);
    return policy;
  }

  /**
   * A constant speculative execution policy with its parameters stubbed; read off the instance for
   * the same reason as the reconnection delays, see {@link #exponentialReconnection}.
   *
   * @param maxExecutions counted the way the policy counts it — including the initial,
   *     non-speculative execution, so the report shows one less.
   */
  private static ConstantSpeculativeExecutionPolicy constantSpeculativeExecution(
      int maxExecutions, long delayMillis) {
    ConstantSpeculativeExecutionPolicy policy = mock(ConstantSpeculativeExecutionPolicy.class);
    when(policy.getMaxExecutions()).thenReturn(maxExecutions);
    when(policy.getConstantDelayMillis()).thenReturn(delayMillis);
    return policy;
  }

  /**
   * A built-in load balancing policy carrying the state it would have latched from the shipped
   * defaults; read off the instance for the same reason as the reconnection delays, see {@link
   * #exponentialReconnection}.
   */
  private static <T extends BasicLoadBalancingPolicy> T loadBalancing(Class<T> policyClass) {
    return loadBalancing(policyClass, /* avoidSlowReplicas= */ true, /* maxNodesPerRemoteDc= */ 0);
  }

  /**
   * Same, with the two latched values set explicitly — so a test can put the profile and the
   * running policy deliberately out of step, the way a configuration reload does.
   *
   * @param avoidSlowReplicas ignored for {@link BasicLoadBalancingPolicy}, which has no
   *     slow-replica-avoidance mechanism to latch.
   */
  private static <T extends BasicLoadBalancingPolicy> T loadBalancing(
      Class<T> policyClass, boolean avoidSlowReplicas, int maxNodesPerRemoteDc) {
    T policy = mock(policyClass);
    when(policy.getMaxNodesPerRemoteDc()).thenReturn(maxNodesPerRemoteDc);
    if (policy instanceof DefaultLoadBalancingPolicy) {
      when(((DefaultLoadBalancingPolicy) policy).isAvoidingSlowReplicas())
          .thenReturn(avoidSlowReplicas);
    }
    return policy;
  }

  /**
   * A timestamp generator that assigns timestamps client-side, which is what every built-in but
   * {@link ServerSideTimestampGenerator} does.
   *
   * <p>A mock of the concrete built-in rather than of the interface: the reporter recognizes the
   * driver's own generators by type, so a bare {@code mock(TimestampGenerator.class)} would be the
   * unrecognized case and silently omit {@code client-timestamps} from every report built through
   * this helper. Mockito's inline mock maker (the default since 5.x) instruments the class itself
   * instead of subclassing it, so the mock satisfies the reporter's {@code instanceof}.
   */
  private static TimestampGenerator clientSideGenerator() {
    return mock(AtomicTimestampGenerator.class);
  }

  private JsonNode report(DefaultDriverConfigReporter reporter) throws Exception {
    return report(reporter, TlsInfo.disabled());
  }

  private JsonNode report(DefaultDriverConfigReporter reporter, TlsInfo tlsInfo) throws Exception {
    return MAPPER.readTree(reporter.buildJson(tlsInfo));
  }

  /** A real default execution profile with the given customizations applied. */
  private DriverExecutionProfile defaults(Consumer<OptionsMap> customizer) {
    OptionsMap map = OptionsMap.driverDefaults();
    customizer.accept(map);
    return DriverConfigLoader.fromMap(map).getInitialConfig().getDefaultProfile();
  }

  /** Reporter over default config + the Java-default policy set. */
  private DefaultDriverConfigReporter defaultsReporter(Consumer<OptionsMap> customizer) {
    return reporterWith(
        defaults(customizer),
        exponentialReconnection(),
        mock(DefaultRetryPolicy.class),
        mock(NoSpeculativeExecutionPolicy.class),
        loadBalancing(DefaultLoadBalancingPolicy.class),
        clientSideGenerator(),
        Optional.empty());
  }

  private DefaultDriverConfigReporter reporterWith(
      DriverExecutionProfile profile,
      ReconnectionPolicy reconnection,
      RetryPolicy retry,
      SpeculativeExecutionPolicy speculative,
      LoadBalancingPolicy loadBalancing,
      TimestampGenerator timestamps,
      Optional<SslEngineFactory> ssl) {
    return reporterWith(
        profile, reconnection, retry, speculative, loadBalancing, timestamps, ssl, null);
  }

  /** Same as the 7-arg overload, with an optional programmatic ({@code withLocalDatacenter}) DC. */
  private DefaultDriverConfigReporter reporterWith(
      DriverExecutionProfile profile,
      ReconnectionPolicy reconnection,
      RetryPolicy retry,
      SpeculativeExecutionPolicy speculative,
      LoadBalancingPolicy loadBalancing,
      TimestampGenerator timestamps,
      Optional<SslEngineFactory> ssl,
      String programmaticLocalDc) {
    // DefaultDriverContext derives the low-level handler factory from the public engine factory;
    // mirror the context shape even though the reporter now reads neither one.
    return reporterWith(
        profile,
        reconnection,
        retry,
        speculative,
        loadBalancing,
        timestamps,
        ssl,
        ssl.map(JdkSslHandlerFactory::new),
        programmaticLocalDc);
  }

  /**
   * Same as the 8-arg overload, with the SSL handler factory set independently of the engine
   * factory — as an override of {@code DefaultDriverContext.buildSslHandlerFactory()} would.
   */
  private DefaultDriverConfigReporter reporterWith(
      DriverExecutionProfile profile,
      ReconnectionPolicy reconnection,
      RetryPolicy retry,
      SpeculativeExecutionPolicy speculative,
      LoadBalancingPolicy loadBalancing,
      TimestampGenerator timestamps,
      Optional<SslEngineFactory> ssl,
      Optional<SslHandlerFactory> sslHandler,
      String programmaticLocalDc) {
    return new DefaultDriverConfigReporter(
        contextWith(
            profile,
            reconnection,
            retry,
            speculative,
            loadBalancing,
            timestamps,
            ssl,
            sslHandler,
            programmaticLocalDc));
  }

  /**
   * The context the reporters above read from. Separate from {@link #reporterWith} only so that a
   * test can build a reporter subclass over it, the way the {@code simpleName} seam needs.
   */
  private InternalDriverContext contextWith(
      DriverExecutionProfile profile,
      ReconnectionPolicy reconnection,
      RetryPolicy retry,
      SpeculativeExecutionPolicy speculative,
      LoadBalancingPolicy loadBalancing,
      TimestampGenerator timestamps,
      Optional<SslEngineFactory> ssl,
      Optional<SslHandlerFactory> sslHandler,
      String programmaticLocalDc) {
    InternalDriverContext ctx = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    when(ctx.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(ctx.getReconnectionPolicy()).thenReturn(reconnection);
    when(ctx.getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(retry);
    when(ctx.getSpeculativeExecutionPolicy(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(speculative);
    when(ctx.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(loadBalancing);
    when(ctx.getTimestampGenerator()).thenReturn(timestamps);
    when(ctx.getSslEngineFactory()).thenReturn(ssl);
    when(ctx.getSslHandlerFactory()).thenReturn(sslHandler);
    when(ctx.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(programmaticLocalDc);
    return ctx;
  }

  /** A minimal {@link DriverContext} good enough to construct a real built-in policy instance. */
  private DriverContext policyConstructionContext() {
    DriverContext ctx = mock(DriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = defaults(map -> {});
    when(ctx.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(config.getProfile(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(profile);
    when(ctx.getSessionName()).thenReturn("test-session");
    return ctx;
  }
}
