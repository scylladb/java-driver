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

import com.datastax.driver.core.policies.ChainableLoadBalancingPolicy;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ErrorAwarePolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.HostFilterPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.PagingOptimizingLoadBalancingPolicy;
import com.datastax.driver.core.policies.PercentileSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RackAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link DriverConfigReporter}: serializes the driver configuration to the cross-driver
 * {@code DRIVER_CONFIG} JSON shape, which {@link Connection.Factory} then sends in the control
 * connection's {@code STARTUP} options.
 *
 * <p>The report is built afresh for every control connection, from inside its {@code STARTUP} frame
 * assembly, and never cached: it describes the objects that are in force at that handshake rather
 * than the configuration the {@link Cluster} was constructed from. That distinction is not
 * theoretical. {@code Cluster.Manager.init()} builds the {@link Connection.Factory} before it calls
 * {@link LoadBalancingPolicy#init}, and the first control connection's {@code STARTUP} goes out in
 * between — so a datacenter or rack the policy infers from the node it reaches cannot be known on
 * the first report and is known on every later one. A reconnect therefore costs one report build
 * and may legitimately report more than the connection before it did.
 *
 * <p>The report follows the normative cross-driver schema (report {@code version} 1): kebab-case
 * keys, nested objects, and <b>omission</b> of any key or group that has no value (nothing is ever
 * emitted as {@code null}). The same applies where a configured value falls outside what the schema
 * can express but the key is <em>optional</em>: a disabled connect timeout, a disabled read
 * timeout, a disabled {@code SO_LINGER}, an unbounded page size and a non-serial default serial
 * consistency level are omitted rather than emitted as a value the schema rejects. Two keys are
 * omitted for a third reason — the schema admits only a boolean, and 3.x cannot observe which one
 * applies: {@code query.defaults.client-timestamps} for a timestamp generator that is none of the
 * driver's own, and {@code connection.tls.hostname-verification} for any {@link SSLOptions} other
 * than {@link SniSSLOptions}. Both keys are documented as absent when the behavior is unknown,
 * which is exactly the case here: a custom generator decides per call whether to assign a timestamp
 * at all, and every other {@code SSLOptions} builds its engine from a user-supplied {@code
 * SSLContext} (or hands the whole handler over to Netty), so nothing the reporter can read says
 * whether the hostname is checked. Two more optional bounds are omitted for the opposite reason —
 * 3.x simply has no such bound to report: {@code connection.reconnection.policy.max-attempts},
 * since its reconnection policies retry forever, and {@code query.retry.policy.max-retries}, since
 * no single number describes a 3.x retry policy: {@link RetryPolicy#onRequestError} is the one path
 * that does not stop after a single retry, and it is only reached for an idempotent statement, so
 * the same policy bounds a non-idempotent request at one retry and an idempotent one at the length
 * of the query plan. Which of the two a statement gets is not configuration — {@link
 * Statement#setIdempotent} overrides the reported {@code query.defaults.idempotence} per statement.
 *
 * <p>Only a token-aware chain has a built-in shape in the schema, whose {@code
 * query.load-balancing.policy.type} admits {@code token-aware} and {@code custom} and nothing else.
 * That shape carries no name, so it is claimed only for a chain every element of which the group
 * can describe; any other chain is {@code custom}, named after every policy in it — {@code
 * WhiteListPolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy))} — and keeps the capability keys it
 * can still state, which the {@code custom} branch admits as additional properties. See {@link
 * #loadBalancingPolicy}. A datacenter and rack are reported in {@code
 * query.load-balancing.node-preference} whenever the chain prefers one: from a DC- or rack-aware
 * policy, or from a filter restricting the session to a single datacenter. A bare {@link
 * RoundRobinPolicy}, or a {@link WhiteListPolicy} over one, prefers neither and both preference
 * keys are omitted.
 *
 * <p>The schema reports that preference in a second, optional place, {@code
 * connection.node-preference}, which describes the part of the cluster the driver holds connections
 * to rather than how a query is routed. One {@link LoadBalancingPolicy} decides both in 3.x, since
 * {@link LoadBalancingPolicy#distance(Host)} is what governs whether a host is pooled at all, so
 * that key is derived from the same chain — but from its datacenter half alone. A rack-aware
 * policy's {@code distance()} returns {@code REMOTE}, never {@code IGNORED}, for a local-datacenter
 * host in another rack, so those hosts are still pooled and the rack does not scope what the driver
 * connects to. The datacenter does: a host outside the preferred one is {@code IGNORED} unless the
 * policy is configured to use hosts there, and an ignored host gets no pool at all.
 *
 * <p>That last claim is exact only for the policy the preference was read from. A chainable policy
 * <em>above</em> it can narrow pooling further: {@link HostFilterPolicy#distance(Host)} — and
 * therefore {@link WhiteListPolicy}'s — returns {@code IGNORED} for any host failing its predicate,
 * including one inside the reported datacenter, and a custom policy computes {@code distance()}
 * itself and need honor nothing the chain below it prefers. The reported datacenter is thus
 * necessary but not sufficient: it is what the located policy prefers, not the whole of what gets a
 * pool. Reported all the same, deliberately, on the grounds that hiding a datacenter the operator
 * really did configure is the worse failure mode — note the asymmetry with the inferred values
 * above, in that nothing is inferred on a third party's behalf while what was explicitly configured
 * is passed through even where a wrapper may override it.
 *
 * <p>A filter that restricts the session to exactly one datacenter is read the other way round:
 * {@link HostFilterPolicy#fromDCWhiteList(LoadBalancingPolicy, Iterable)} retains the datacenters
 * it was given, so when nothing in the chain prefers a datacenter of its own — {@code
 * fromDCWhiteList(new RoundRobinPolicy(), ["dc1"])} — the restriction is what decides the
 * preference and is reported as one. Anything a filter can express that names no single datacenter
 * keeps the group omitted: a blacklist, several allowed datacenters, a blank name, a {@link
 * WhiteListPolicy} (which filters on addresses), or a caller-supplied {@code Predicate<Host>},
 * which stays opaque. A filter the preference was <em>not</em> read from — because a location-aware
 * policy below it won — narrows the chain without being stated anywhere, so it costs the chain its
 * built-in shape and is named instead, exactly like any other restricting wrapper.
 *
 * <p><b>Known limitations:</b> omission is not always available, because these fields are
 * <em>required</em> by the schema.
 *
 * <ul>
 *   <li>{@code connection.requests.in-flight.max} must be positive, while {@link
 *       PoolingOptions#setMaxRequestsPerConnection(HostDistance, int)} also accepts 0.
 *   <li>{@code query.speculative-execution.policy.percentile} is bounded to 0..100 exclusive by the
 *       schema, while {@link PercentileSpeculativeExecutionPolicy} accepts a percentile of 0.
 * </ul>
 *
 * <p>{@code connection.requests.orphaned.max} used to belong to that list and no longer does: 3.x
 * has no orphaned-request setting to report it from — a request the driver stopped waiting for
 * keeps its stream identifier until the response arrives, with no configurable bound and no
 * connection replacement (only 4.x has {@code advanced.connection.max-orphan-requests}) — and the
 * schema now makes the group optional for exactly that case, so omission is the schema-valid
 * answer.
 *
 * <p>Such a value is reported <em>as-is</em>, and a key with no equivalent stays omitted: the
 * reporter deliberately neither fabricates an in-range value — which would misreport a setting an
 * operator may have chosen on purpose, or a policy 3.x does not implement — nor drops the whole
 * report over one field, so the document is accurate but fails schema validation. Tracked as a
 * cross-driver schema gap: the fix is to let those fields express the value, the way {@code
 * control-plane.schema.agreement.timeout-ms} already admits 0.
 *
 * <p>The read timeout feeds three places, all of them optional, so disabling it omits all three:
 * {@code connection.read}, {@code control-plane.queries.system.timeout.client-side-ms} and {@code
 * query.defaults.request}.
 */
public class DefaultDriverConfigReporter implements DriverConfigReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDriverConfigReporter.class);

  /** STARTUP option key under which the config JSON is sent. */
  public static final String DRIVER_CONFIG_KEY = "DRIVER_CONFIG";

  /**
   * Major schema version. Adding keys is backward-compatible and does not bump this; only
   * changing/removing the meaning of an existing key does.
   */
  static final int SCHEMA_VERSION = 1;

  /**
   * Upper bound on the UTF-8 size of the {@code DRIVER_CONFIG} value; a longer report is dropped
   * rather than sent.
   *
   * <p>{@code STARTUP} options are serialized with {@code CBUtil.writeStringMap}, which writes each
   * value with a 16-bit length prefix and no bounds check: a value longer than 65535 bytes would
   * silently truncate that prefix modulo 65536 while still appending the whole body, corrupting the
   * frame and failing the handshake. Note that nothing throws on that path, so it is not a failure
   * the {@code try/catch} in {@link #buildReport()} could contain.
   *
   * <p>Most of this report is fixed-shape, but some of it is user-supplied and unbounded —
   * datacenter and rack names, consistency levels, and the class names of custom policy objects —
   * so enforcing a limit here keeps "reporting must never prevent a connection from being
   * established" a property of this class rather than of the user's configuration. 32KiB is
   * generous for a configuration report, and is the same limit the other ScyllaDB drivers apply.
   */
  static final int MAX_DRIVER_CONFIG_LENGTH = 32 * 1024;

  /**
   * Upper bound on the number of policies visited while walking a load balancing policy chain.
   *
   * <p>The walk follows {@code ChainableLoadBalancingPolicy.getChildPolicy()} on arbitrary
   * user-supplied policy objects, so a policy that returns itself — or any cycle — would otherwise
   * spin forever on the {@link Cluster} initialization path. That is the one failure mode the
   * {@code try/catch} in {@link #buildReport()} cannot contain, because it hangs rather than
   * throws.
   *
   * <p>The built-in chains are a handful of policies deep at most, so hitting this bound means a
   * malformed chain rather than a legitimately deep one.
   */
  private static final int MAX_POLICY_CHAIN_LENGTH = 16;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final Configuration configuration;

  public DefaultDriverConfigReporter(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String buildReport() {
    // Configuration reporting is a best-effort diagnostic aid, so any failure here (a bad config
    // read, a misbehaving policy while introspecting, a serialization error) must be swallowed
    // rather than allowed to propagate: it is built on the Cluster-initialization path, which must
    // not fail because of a diagnostic. Also catches InternalError specifically: customPolicy()
    // calls getClass().getSimpleName() on arbitrary user-supplied policy objects, which has a
    // documented JDK edge case throwing InternalError for certain synthetic classes. Deliberately
    // not a bare `Error` — that would also swallow OutOfMemoryError/StackOverflowError, masking a
    // real JVM-level failure instead of this one narrow, documented case.
    try {
      String json = buildJson();
      if (json == null) {
        return null;
      }
      // Measured on the encoded bytes, since that is what the length prefix on the wire counts.
      int length = json.getBytes(StandardCharsets.UTF_8).length;
      if (length > MAX_DRIVER_CONFIG_LENGTH) {
        LOGGER.warn(
            "The driver configuration report is {} bytes long, which exceeds the {} byte limit; "
                + "skipping DRIVER_CONFIG",
            length,
            MAX_DRIVER_CONFIG_LENGTH);
        return null;
      }
      return json;
    } catch (InternalError | RuntimeException e) {
      LOGGER.warn(
          "Error while building the driver configuration report; skipping driver config reporting",
          e);
      return null;
    }
  }

  /** Builds the compact, single-line JSON configuration report. */
  protected String buildJson() {
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    root.put("version", SCHEMA_VERSION);
    populateConfig(root);
    try {
      return OBJECT_MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      // An in-memory node tree should never fail to serialize; never let it break connection setup.
      LOGGER.warn("Failed to serialize driver configuration report; skipping DRIVER_CONFIG", e);
      return null;
    }
  }

  /**
   * Populates the three top-level configuration groups onto the report root from {@link
   * #configuration} and its policies. Keys the driver has no equivalent for (or cannot introspect
   * in 3.x) are omitted rather than emitted as {@code null}.
   */
  protected void populateConfig(ObjectNode root) {
    Policies policies = configuration.getPolicies();
    // The load balancing policy chain feeds three keys across two groups, so it is walked once here
    // and handed to both: the policy object and the full node preference under "query", and the
    // datacenter half of that preference under "connection".
    List<LoadBalancingPolicy> lbChain = policyChain(policies.getLoadBalancingPolicy());
    NodeLocation nodeLocation = nodeLocation(lbChain);
    root.set("connection", connection(policies, nodeLocation));
    root.set("control-plane", controlPlane());
    root.set("query", query(policies, lbChain, nodeLocation));
  }

  private ObjectNode connection(Policies policies, NodeLocation nodeLocation) {
    SocketOptions socketOptions = configuration.getSocketOptions();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // The "connect" group is required but its timeout is not, and a non-positive connect timeout
    // disables it: omit the timeout rather than report a number the schema rejects, leaving the
    // group empty.
    ObjectNode connect = OBJECT_MAPPER.createObjectNode();
    int connectTimeoutMillis = socketOptions.getConnectTimeoutMillis();
    if (connectTimeoutMillis > 0) {
      connect.put("timeout-ms", connectTimeoutMillis);
    }
    n.set("connect", connect);
    // Optional group, positive-only: a non-positive read timeout disables read timeouts, so omit
    // the group rather than report a number the schema rejects.
    int readTimeoutMillis = socketOptions.getReadTimeoutMillis();
    if (readTimeoutMillis > 0) {
      n.set("read", OBJECT_MAPPER.createObjectNode().put("timeout-ms", readTimeoutMillis));
    }
    // No socket-level write timeout in 3.x -> omit "write". "heartbeat" is a reserved-empty
    // placeholder in this schema version (the heartbeat interval has no home yet) -> omit.
    n.set("requests", requests());
    n.set("pool", pool());
    n.set("socket", socket());
    n.set("reconnection", wrap("policy", reconnectionPolicy(policies)));
    // Optional, and absent when TLS is disabled rather than reported as off.
    ObjectNode tls = tls();
    if (tls != null) {
      n.set("tls", tls);
    }
    // Optional, and the datacenter half of the preference only -- the rack does not scope which
    // hosts are pooled; see the class javadoc.
    if (nodeLocation != null) {
      n.set("node-preference", nodeLocation.toDatacenterPreference());
    }
    return n;
  }

  /**
   * The {@code query} group: the per-request defaults plus the three policies that act on a query,
   * each nested under the group the schema gives it.
   */
  private ObjectNode query(
      Policies policies, List<LoadBalancingPolicy> lbChain, NodeLocation nodeLocation) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    n.set("defaults", queryDefaults(policies));
    // 3.x retries immediately: no built-in retry policy inserts a delay between attempts, and a
    // custom one exposes no schedule to introspect -> omit the optional "backoff".
    n.set("retry", wrap("policy", retryPolicy(policies)));

    ObjectNode loadBalancing =
        wrap(
            "policy",
            loadBalancingPolicy(lbChain, policies.getLoadBalancingPolicy(), nodeLocation));
    // Optional: omitted when the LB policy carries no DC/rack notion.
    if (nodeLocation != null) {
      loadBalancing.set("node-preference", nodeLocation.toFullPreference());
    }
    n.set("load-balancing", loadBalancing);

    // Optional: omitted when there is no speculative execution.
    ObjectNode specEx = speculativeExecutionPolicy(policies);
    if (specEx != null) {
      n.set("speculative-execution", wrap("policy", specEx));
    }
    return n;
  }

  private ObjectNode requests() {
    PoolingOptions pooling = configuration.getPoolingOptions();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    n.set(
        "in-flight",
        OBJECT_MAPPER
            .createObjectNode()
            .put(
                "max",
                effective(
                    pooling.getMaxRequestsPerConnection(HostDistance.LOCAL),
                    poolDefault(PoolingOptions.MAX_REQUESTS_PER_CONNECTION_LOCAL_KEY))));
    // "orphaned" has no 3.x equivalent, so it is omitted; the schema makes the group optional for
    // exactly that case — see the class javadoc.
    return n;
  }

  private ObjectNode pool() {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // The schema carries no pool size or keying: 3.x has a single per-host pool type, and a
    // per-shard count could not be reported anyway, since this report is built before the control
    // connection is up and no node's shard count is known yet.
    n.set(
        "shard-aware",
        OBJECT_MAPPER
            .createObjectNode()
            .put("enabled", configuration.getProtocolOptions().isUseAdvancedShardAwareness()));
    return n;
  }

  private ObjectNode socket() {
    SocketOptions o = configuration.getSocketOptions();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // Boolean options report the effective on/off state; when the driver leaves them unset the
    // OS/platform default applies, approximated here (tcp-no-delay defaults on, the others off).
    // Best-effort: Connection.Factory applies each option only when SocketOptions has a value for
    // it, then hands the bootstrap to NettyOptions.afterBootstrapInitialized, which can set any
    // ChannelOption afterwards without this being able to see it.
    n.put("tcp-no-delay", boolOrDefault(o.getTcpNoDelay(), true));
    n.put("keep-alive", boolOrDefault(o.getKeepAlive(), false));
    n.put("reuse-address", boolOrDefault(o.getReuseAddress(), false));
    // All three groups below are optional, so a value the schema cannot express is omitted rather
    // than emitted: a negative SO_LINGER means lingering close is disabled (the schema takes a
    // non-negative interval, so 0 is still reported), and a non-positive buffer size leaves the
    // JDK/OS default in place (the schema takes a positive size).
    Integer soLinger = o.getSoLinger();
    if (soLinger != null && soLinger >= 0) {
      n.set("linger", OBJECT_MAPPER.createObjectNode().put("interval-s", soLinger));
    }
    Integer receiveBufferSize = o.getReceiveBufferSize();
    if (receiveBufferSize != null && receiveBufferSize > 0) {
      n.set(
          "receive-buffer", OBJECT_MAPPER.createObjectNode().put("size-bytes", receiveBufferSize));
    }
    Integer sendBufferSize = o.getSendBufferSize();
    if (sendBufferSize != null && sendBufferSize > 0) {
      n.set("send-buffer", OBJECT_MAPPER.createObjectNode().put("size-bytes", sendBufferSize));
    }
    return n;
  }

  private ObjectNode controlPlane() {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // 3.x has no dedicated control-connection timeout; internal/system queries use the read
    // timeout.
    // There is no client-configurable server-side ("USING TIMEOUT") timeout -> omit server-side-ms.
    ObjectNode timeout = OBJECT_MAPPER.createObjectNode();
    // Optional and positive-only, and a non-positive read timeout disables read timeouts: omit
    // rather than report a number the schema rejects. The enclosing "timeout" object is required,
    // so it stays (empty).
    int clientSideMs = configuration.getSocketOptions().getReadTimeoutMillis();
    if (clientSideMs > 0) {
      timeout.put("client-side-ms", clientSideMs);
    }
    n.set("queries", wrap("system", wrap("timeout", timeout)));
    // Required and non-negative in the schema, and 0 is meaningful (do not wait for agreement).
    // Cluster.Builder rejects a non-positive wait, but ProtocolOptions can be constructed with one
    // directly, and a negative wait behaves exactly like 0 — so normalizing it is exact rather than
    // invented, and keeps the required field in range.
    long schemaAgreementMs =
        Math.max(0L, configuration.getProtocolOptions().getMaxSchemaAgreementWaitSeconds() * 1000L);
    n.set(
        "schema",
        wrap("agreement", OBJECT_MAPPER.createObjectNode().put("timeout-ms", schemaAgreementMs)));
    return n;
  }

  private ObjectNode reconnectionPolicy(Policies policies) {
    ReconnectionPolicy policy = policies.getReconnectionPolicy();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    if (policy instanceof ExponentialReconnectionPolicy) {
      ExponentialReconnectionPolicy p = (ExponentialReconnectionPolicy) policy;
      n.put("type", "exponential");
      n.put("base-ms", p.getBaseDelayMs());
      n.put("max-ms", p.getMaxDelayMs());
      // Optional, and omitted because 3.x built-in reconnection policies never give up. The
      // maxAttempts field ExponentialReconnectionPolicy carries is not that bound: it is an
      // overflow
      // guard on the doubling, and past it nextDelayMs() keeps returning maxDelayMs forever.
    } else if (policy instanceof ConstantReconnectionPolicy) {
      n.put("type", "constant");
      n.put("delay-ms", ((ConstantReconnectionPolicy) policy).getConstantDelayMs());
    } else {
      customPolicy(n, policy);
    }
    return n;
  }

  private ObjectNode retryPolicy(Policies policies) {
    RetryPolicy policy = policies.getRetryPolicy();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // No 3.x policy has a retry count to report, so the optional "max-retries" is always omitted;
    // both built-ins below are parameterless singletons, and the schema's own wording for the key
    // is "absent when no explicit retry limit is configured". Nor could one be derived: they stop
    // after one attempt on a read timeout, a write timeout or an unavailable error -- all three
    // sharing one counter, so one retry between them, not one each -- while onRequestError leaves
    // nbRetry unread and keeps trying the next host until the query plan runs out. Which of those
    // applies is decided per statement rather than by configuration: RequestHandler only consults
    // onRequestError and onWriteTimeout for an idempotent statement, so the same policy bounds a
    // non-idempotent request at one retry and an idempotent one at the length of the query plan.
    // FallthroughRetryPolicy retries nothing, and the schema's "fallthrough" admits no such key
    // anyway; a wrapper reported as custom cannot be introspected at all.
    if (policy instanceof DefaultRetryPolicy) {
      n.put("type", "standard-error-aware");
    } else if (policy instanceof DowngradingConsistencyRetryPolicy) {
      n.put("type", "downgrading-consistency");
    } else if (policy instanceof FallthroughRetryPolicy) {
      n.put("type", "fallthrough");
    } else {
      // LoggingRetryPolicy / IdempotenceAwareRetryPolicy wrap a child but expose no getter, so only
      // the outer type can be reported.
      customPolicy(n, policy);
    }
    return n;
  }

  private ObjectNode speculativeExecutionPolicy(Policies policies) {
    SpeculativeExecutionPolicy policy = policies.getSpeculativeExecutionPolicy();
    if (policy instanceof NoSpeculativeExecutionPolicy) {
      return null;
    }
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    if (policy instanceof ConstantSpeculativeExecutionPolicy) {
      ConstantSpeculativeExecutionPolicy p = (ConstantSpeculativeExecutionPolicy) policy;
      n.put("type", "constant");
      n.put("max-executions", p.getMaxSpeculativeExecutions());
      n.put("delay-ms", p.getConstantDelayMillis());
    } else if (policy instanceof PercentileSpeculativeExecutionPolicy) {
      PercentileSpeculativeExecutionPolicy p = (PercentileSpeculativeExecutionPolicy) policy;
      n.put("type", "percentile");
      n.put("max-executions", p.getMaxSpeculativeExecutions());
      // Required, so a percentile of 0 — which the schema's range does not admit but the policy
      // accepts — is reported as-is; see the class javadoc.
      n.put("percentile", p.getPercentile());
    } else {
      customPolicy(n, policy);
    }
    return n;
  }

  /**
   * The {@code query.load-balancing.policy} group for a chain, discriminated on whether the schema
   * can describe every element of it.
   *
   * <p>The built-in {@code token-aware} shape has no room for a name, so claiming it means claiming
   * the chain is <em>exactly</em> that shape. Each element is therefore classified: {@link
   * TokenAwarePolicy} and {@link LatencyAwarePolicy} are capabilities the shape carries as {@code
   * load-distribution} and {@code adaptive-ordering}; {@link DCAwareRoundRobinPolicy}, {@link
   * RackAwareRoundRobinPolicy} and {@link RoundRobinPolicy} are described by {@code
   * node-preference} and {@code fallback-to-non-preferred-nodes} beside it; {@link
   * PagingOptimizingLoadBalancingPolicy} is an internal wrapper that describes nothing and is
   * dropped; the {@link HostFilterPolicy} the reported {@code node-preference} was read from is
   * described by that preference (see {@link #nodeLocation}). Anything else — {@link
   * WhiteListPolicy}, {@link ErrorAwarePolicy}, an opaque filter, a user policy — restricts or
   * reorders candidates in a way nothing in the group states.
   *
   * <p>A filter is only described by the preference when the preference is <em>its</em>
   * restriction, which is why this is tested against {@code nodeLocation} rather than against the
   * filter alone. A location-aware policy below it wins that slot, and the filter then narrows the
   * chain further without appearing anywhere: {@code fromDCWhiteList(DCAware("dc1"), ["dc2"])}
   * reports {@code dc1} while nothing outside {@code dc2} is reachable. Treating such a filter as
   * described would leave it invisible behind an inner {@link TokenAwarePolicy} and visible without
   * one — the nesting-dependent blind spot this whole classification exists to remove.
   *
   * <p>One such element and the whole chain is reported as {@code custom}, whose {@code name} names
   * every policy involved, outermost first: {@code
   * WhiteListPolicy(TokenAwarePolicy(DCAwareRoundRobinPolicy))}. The capability keys are kept
   * alongside it — the {@code custom} branch admits additional properties precisely so a driver
   * that can introspect a policy may serialize what it knows — so naming the wrapper costs nothing
   * that was previously reported. It was previously invisible: an outer {@code WhiteListPolicy}
   * over a token-aware chain reported plain {@code token-aware}, so whether an operator could see
   * that the client is pinned to a host list depended on an unrelated nesting choice.
   */
  private ObjectNode loadBalancingPolicy(
      List<LoadBalancingPolicy> chain, LoadBalancingPolicy policy, NodeLocation nodeLocation) {
    TokenAwarePolicy tokenAware = null;
    boolean latencyAware = false;
    DCAwareRoundRobinPolicy dcAware = null;
    RackAwareRoundRobinPolicy rackAware = null;
    boolean describedByTheSchema = true;
    for (LoadBalancingPolicy current : chain) {
      if (current instanceof TokenAwarePolicy) {
        tokenAware = (TokenAwarePolicy) current;
      } else if (current instanceof LatencyAwarePolicy) {
        latencyAware = true;
      } else if (current instanceof DCAwareRoundRobinPolicy) {
        dcAware = (DCAwareRoundRobinPolicy) current;
      } else if (current instanceof RackAwareRoundRobinPolicy) {
        rackAware = (RackAwareRoundRobinPolicy) current;
      } else if (!(current instanceof PagingOptimizingLoadBalancingPolicy
          || current instanceof RoundRobinPolicy
          || (nodeLocation != null && current == nodeLocation.source))) {
        describedByTheSchema = false;
      }
    }

    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    if (describedByTheSchema && tokenAware != null) {
      n.put("type", "token-aware");
    } else {
      n.put("type", "custom");
      n.put("name", chainName(chain, policy));
    }
    if (tokenAware != null) {
      n.put("load-distribution", loadDistribution(tokenAware.getReplicaOrdering()));
      // A rack-aware policy always reaches the other racks of its local datacenter -- the second
      // tier of a normal query plan, once the local rack is exhausted, and folded into the first
      // tier for an LWT or serial-consistency statement, which skips rack prioritization altogether
      // -- and its distance() returns REMOTE, not IGNORED, for them, so its preference is always
      // fallen back on either way. A DC-aware policy only leaves the preferred datacenter when
      // configured to use hosts there.
      boolean fallback =
          rackAware != null || (dcAware != null && dcAware.getUsedHostsPerRemoteDc() > 0);
      n.put("fallback-to-non-preferred-nodes", fallback);
    }
    // Optional, and its "signals" cannot be empty: the group is omitted altogether when nothing
    // reorders candidates at runtime. Latency, via LatencyAwarePolicy, is the only observation a
    // 3.x policy can reorder them on, and it is reported wherever that policy appears -- adaptive
    // ordering is a property of the chain, not of token awareness, so LatencyAwarePolicy over a
    // bare RoundRobinPolicy carries it just as it does over a token-aware one.
    if (latencyAware) {
      ObjectNode adaptiveOrdering = OBJECT_MAPPER.createObjectNode();
      adaptiveOrdering.putArray("signals").add("latency");
      n.set("adaptive-ordering", adaptiveOrdering);
    }
    return n;
  }

  /**
   * Every policy in {@code chain} rendered outermost-first as {@code Outer(Inner(Innermost))},
   * skipping the internal {@code PagingOptimizingLoadBalancingPolicy} wrapper {@code
   * Cluster.Manager} puts around every session's policy — reporting that would tell an operator
   * nothing about what the client runs.
   */
  private static String chainName(List<LoadBalancingPolicy> chain, LoadBalancingPolicy fallback) {
    StringBuilder name = new StringBuilder();
    int open = 0;
    for (LoadBalancingPolicy current : chain) {
      if (current instanceof PagingOptimizingLoadBalancingPolicy) {
        continue;
      }
      if (open > 0) {
        name.append('(');
      }
      name.append(policyName(current));
      open++;
    }
    if (open == 0) {
      return policyName(fallback);
    }
    for (int i = 1; i < open; i++) {
      name.append(')');
    }
    return name.toString();
  }

  /**
   * The schema's normalized name for how a token-aware policy distributes requests over the
   * replicas it considers equally preferred.
   */
  private static String loadDistribution(TokenAwarePolicy.ReplicaOrdering replicaOrdering) {
    switch (replicaOrdering) {
      case RANDOM:
        // A different random order for each query plan.
        return "shuffle";
      case TOPOLOGICAL:
        // The replica set's own order around the token ring, never reordered.
        return "replica-set";
      case NEUTRAL:
      default:
        // Whatever order the child policy's query plan has, i.e. round-robin for every built-in
        // child, each of which rotates its starting host across successive plans.
        return "round-robin";
    }
  }

  /**
   * The node location {@code chain} prefers, or {@code null} when no policy in it carries one — a
   * bare {@link RoundRobinPolicy}, or a custom policy with no introspectable location — in which
   * case both of the schema's node preference keys are omitted.
   *
   * <p>A location-aware policy wins over a filter above it: {@code
   * fromDCWhiteList(DCAwareRoundRobinPolicy(...))} reports the policy's own datacenter, which is
   * what routes its query plans. The filter is consulted only when nothing in the chain prefers a
   * datacenter of its own, and only when it restricts the session to exactly one — see {@link
   * #singleWhiteListedDatacenter}.
   */
  private static NodeLocation nodeLocation(List<LoadBalancingPolicy> chain) {
    DCAwareRoundRobinPolicy dcAware = null;
    RackAwareRoundRobinPolicy rackAware = null;
    HostFilterPolicy dcFilter = null;
    String filteredDc = null;
    for (LoadBalancingPolicy current : chain) {
      if (current instanceof DCAwareRoundRobinPolicy) {
        dcAware = (DCAwareRoundRobinPolicy) current;
      } else if (current instanceof RackAwareRoundRobinPolicy) {
        rackAware = (RackAwareRoundRobinPolicy) current;
      } else if (current instanceof HostFilterPolicy) {
        String dc = singleWhiteListedDatacenter((HostFilterPolicy) current);
        if (dc != null) {
          dcFilter = (HostFilterPolicy) current;
          filteredDc = dc;
        }
      }
    }
    if (rackAware != null) {
      return NodeLocation.ofRack(
          rackAware,
          rackAware.getLocalDc(),
          rackAware.isLocalDcExplicit(),
          rackAware.getLocalRack(),
          rackAware.isLocalRackExplicit());
    }
    if (dcAware != null) {
      return NodeLocation.ofDatacenter(dcAware, dcAware.getLocalDc(), dcAware.isLocalDcExplicit());
    }
    if (dcFilter != null) {
      // Explicit: the datacenter was named by the caller, not inferred from the node the driver
      // happened to reach first.
      return NodeLocation.ofDatacenter(dcFilter, filteredDc, true);
    }
    return null;
  }

  /**
   * The one datacenter {@code policy} restricts the session to, or {@code null} when it names none,
   * several, or something that is not a datacenter at all: a denied datacenter names no preferred
   * one, several allowed datacenters name no single one, and a {@link WhiteListPolicy} (or a
   * caller-supplied predicate) filters on something else entirely — {@link
   * HostFilterPolicy#getWhiteListedDatacenters()} is empty for all of those.
   *
   * <p>A blank name is no name either. Nothing validates the strings handed to {@link
   * HostFilterPolicy#fromDCWhiteList(LoadBalancingPolicy, Iterable)}, and the schema requires a
   * non-empty {@code local-dc} beside {@code type: "dc"} — so the whole preference is omitted
   * rather than reported as a value the schema rejects, which is also how {@link
   * DCAwareRoundRobinPolicy#getLocalDc()} and {@link RackAwareRoundRobinPolicy#getLocalRack()}
   * treat one. A merely padded name is not blank and is reported verbatim: hiding the whitespace
   * would hide the typo the report exists to expose.
   */
  private static String singleWhiteListedDatacenter(HostFilterPolicy policy) {
    Set<String> dcs = policy.getWhiteListedDatacenters();
    if (dcs.size() != 1) {
      return null;
    }
    String dc = dcs.iterator().next();
    return Strings.isNullOrEmpty(dc) ? null : dc;
  }

  /**
   * The datacenter and rack a load balancing policy chain prefers, and whether each of them was
   * configured or is left for the policy to infer.
   *
   * <p>Extracted once and rendered twice, because the schema's two node preference keys make
   * different claims about the same policy: {@link #toFullPreference()} for the preference the
   * policy routes queries by, {@link #toDatacenterPreference()} for the part of it that decides
   * which hosts are pooled at all.
   */
  private static final class NodeLocation {

    /**
     * The policy in the chain this location was read from. {@link #loadBalancingPolicy} needs it to
     * tell a filter whose restriction the report states from one that narrows the chain further
     * without being stated anywhere; only the former can be left out of the policy's name.
     */
    private final LoadBalancingPolicy source;

    private final String dc;
    private final boolean dcExplicit;
    private final String rack;
    private final boolean rackExplicit;
    private final boolean rackAware;

    static NodeLocation ofDatacenter(LoadBalancingPolicy source, String dc, boolean dcExplicit) {
      return new NodeLocation(source, dc, dcExplicit, null, false, false);
    }

    static NodeLocation ofRack(
        LoadBalancingPolicy source,
        String dc,
        boolean dcExplicit,
        String rack,
        boolean rackExplicit) {
      return new NodeLocation(source, dc, dcExplicit, rack, rackExplicit, true);
    }

    private NodeLocation(
        LoadBalancingPolicy source,
        String dc,
        boolean dcExplicit,
        String rack,
        boolean rackExplicit,
        boolean rackAware) {
      this.source = source;
      this.dc = dc;
      this.dcExplicit = dcExplicit;
      this.rack = rack;
      this.rackExplicit = rackExplicit;
      this.rackAware = rackAware;
    }

    /** The datacenter half alone, for {@code connection.node-preference}. */
    ObjectNode toDatacenterPreference() {
      ObjectNode n = OBJECT_MAPPER.createObjectNode();
      // "dc" requires the name and always has one: both policies derive the explicitness flag from
      // the configured string, so an explicit datacenter is never blank. "dc-auto" carries an
      // inferred name under plain "local-dc" rather than an "inferred-" prefixed key, unlike
      // "rack-auto" below; that asymmetry is the schema's.
      n.put("type", dcExplicit ? "dc" : "dc-auto");
      putIfNotNull(n, "local-dc", dc);
      return n;
    }

    /** The whole preference, rack included, for {@code query.load-balancing.node-preference}. */
    ObjectNode toFullPreference() {
      if (!rackAware) {
        // A DC-aware policy has no rack notion, so its whole preference is the datacenter.
        return toDatacenterPreference();
      }
      ObjectNode n = OBJECT_MAPPER.createObjectNode();
      if (dcExplicit && rackExplicit) {
        n.put("type", "rack");
        putIfNotNull(n, "local-dc", dc);
        putIfNotNull(n, "local-rack", rack);
        return n;
      }
      // At least one part is inferred, and the schema reports configured and inferred values under
      // separate keys — of which it admits only one per part. An inferred value is only known once
      // the policy has been initialized, i.e. never at report time, so in practice its key is
      // absent.
      n.put("type", "rack-auto");
      putIfNotNull(n, dcExplicit ? "local-dc" : "inferred-local-dc", dc);
      putIfNotNull(n, rackExplicit ? "local-rack" : "inferred-local-rack", rack);
      return n;
    }
  }

  private ObjectNode queryDefaults(Policies policies) {
    QueryOptions q = configuration.getQueryOptions();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // Paging is unbounded either way the driver can express it, and the schema has no sentinel for
    // that ("absent when page is not limited"), so this optional group is omitted entirely.
    // Integer.MAX_VALUE is the documented way to disable paging (QueryOptions#setFetchSize), and is
    // the value that actually reaches a session; a non-positive size is rejected by that setter and
    // only a QueryOptions subclass overriding the getter can still produce one.
    int fetchSize = q.getFetchSize();
    if (fetchSize > 0 && fetchSize != Integer.MAX_VALUE) {
      n.set("page", OBJECT_MAPPER.createObjectNode().put("size", fetchSize));
    }
    // Required, and the schema's enum admits every level QueryOptions accepts, serial ones
    // included. The setter rejects null, so only a QueryOptions subclass overriding the getter can
    // still return one; guarded because letting it through would throw here and cost the whole
    // report, not just this key.
    ConsistencyLevel consistency = q.getConsistencyLevel();
    if (consistency != null) {
      n.put("consistency", consistency.name());
    }
    // Optional, and unlike Statement.setSerialConsistencyLevel, QueryOptions does not check that
    // the level is serial: omit one the schema's enum cannot express rather than emit it.
    ConsistencyLevel serialConsistency = q.getSerialConsistencyLevel();
    if (serialConsistency != null && serialConsistency.isSerial()) {
      n.put("serial-consistency", serialConsistency.name());
    }
    n.put("idempotence", q.getDefaultIdempotence());
    // Optional, and reported only for the driver's own generators, whose answer is fixed:
    // ServerSideTimestampGenerator always returns Long.MIN_VALUE and so always leaves the timestamp
    // to the coordinator, while an AbstractMonotonicTimestampGenerator always computes one
    // client-side (its documented extension point is onDrift, not next). Any other generator
    // decides per call, so nothing here can say which happens -- the "absent when this behavior is
    // unknown" case the schema's optional key is for.
    TimestampGenerator timestampGenerator = policies.getTimestampGenerator();
    if (timestampGenerator instanceof ServerSideTimestampGenerator) {
      n.put("client-timestamps", false);
    } else if (timestampGenerator instanceof AbstractMonotonicTimestampGenerator) {
      n.put("client-timestamps", true);
    }
    // 3.x has no per-request timeout of its own: the read timeout bounds every request, so it is
    // also what connection.read and control-plane.queries.system report. Both the timeout and its
    // enclosing group are optional and the timeout is positive-only, so a disabled read timeout
    // omits the group entirely rather than reporting it empty.
    int requestTimeoutMs = configuration.getSocketOptions().getReadTimeoutMillis();
    if (requestTimeoutMs > 0) {
      n.set("request", OBJECT_MAPPER.createObjectNode().put("timeout-ms", requestTimeoutMs));
    }
    return n;
  }

  /** The {@code tls} group, or {@code null} when TLS is disabled and the group is omitted. */
  private ObjectNode tls() {
    SSLOptions sslOptions = configuration.getProtocolOptions().getSSLOptions();
    if (sslOptions == null) {
      return null;
    }
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // 3.x exposes no hostname-verification flag. SniSSLOptions is the one implementation that
    // verifies the hostname and it hard-codes it on, so it is the only case that can be reported.
    // Every other SSLOptions builds its engine from a user-supplied SSLContext, or hands the whole
    // handler to Netty, and neither says whether endpoint identification is enabled -- which is the
    // "absent when this behavior is unknown" case the schema's optional key is for. The group can
    // therefore be empty: its presence is what reports that TLS is on.
    if (sslOptions instanceof SniSSLOptions) {
      n.put("hostname-verification", true);
    }
    return n;
  }

  /**
   * Returns the load balancing policy chain, outermost policy first, by following {@code
   * ChainableLoadBalancingPolicy.getChildPolicy()} for at most {@link #MAX_POLICY_CHAIN_LENGTH}
   * policies. The chain is walked rather than the configured policy read directly because {@code
   * Cluster.Manager} wraps that policy at runtime; {@code query.load-balancing.policy}, {@code
   * query.load-balancing.node-preference} and {@code connection.node-preference} are all derived
   * from it.
   */
  private static List<LoadBalancingPolicy> policyChain(LoadBalancingPolicy policy) {
    List<LoadBalancingPolicy> chain = new ArrayList<LoadBalancingPolicy>();
    LoadBalancingPolicy current = policy;
    while (current != null && chain.size() < MAX_POLICY_CHAIN_LENGTH) {
      chain.add(current);
      current =
          current instanceof ChainableLoadBalancingPolicy
              ? ((ChainableLoadBalancingPolicy) current).getChildPolicy()
              : null;
    }
    if (current != null) {
      // Only reachable from a user policy whose getChildPolicy() chain is cyclic or absurdly deep;
      // report what was seen rather than walking forever or dropping the whole report.
      LOGGER.warn(
          "Stopped walking the load balancing policy chain after {} policies; reporting only those. "
              + "Does a ChainableLoadBalancingPolicy in the chain return a cyclic child policy?",
          MAX_POLICY_CHAIN_LENGTH);
    }
    return chain;
  }

  private static void customPolicy(ObjectNode node, Object policy) {
    node.put("type", "custom");
    node.put("name", policyName(policy));
  }

  /**
   * The name to report a policy object under: its simple class name, or its binary name when the
   * simple name is empty. An anonymous class has no simple name, and the schema requires a
   * non-empty one.
   */
  private static String policyName(Object policy) {
    Class<?> policyClass = policy.getClass();
    String simpleName = policyClass.getSimpleName();
    return simpleName.isEmpty() ? policyClass.getName() : simpleName;
  }

  /**
   * A new object node holding {@code value} under {@code key}. The schema nests most values one or
   * two single-key objects deep — {@code connection.reconnection.policy}, {@code
   * control-plane.queries.system.timeout} — which this keeps readable.
   */
  private static ObjectNode wrap(String key, JsonNode value) {
    ObjectNode node = OBJECT_MAPPER.createObjectNode();
    node.set(key, value);
    return node;
  }

  private static void putIfNotNull(ObjectNode node, String key, String value) {
    if (value != null) {
      node.put(key, value);
    }
  }

  private static boolean boolOrDefault(Boolean value, boolean defaultValue) {
    return value == null ? defaultValue : value;
  }

  /**
   * The effective pooling default for {@code key}. Needed as a fallback because {@link
   * PoolingOptions} returns {@link PoolingOptions#UNSET} until the protocol version is known, which
   * only happens once the control connection is up — after this report is built. A value the user
   * configured explicitly takes precedence.
   *
   * <p>Resolved with the same walk {@code PoolingOptions.setProtocolVersion} applies once the
   * version is negotiated: the highest {@link PoolingOptions#DEFAULTS} key that does not exceed it.
   * The version is the one the user pinned with {@link
   * Cluster.Builder#withProtocolVersion(ProtocolVersion)}, which is known here, and otherwise
   * {@link ProtocolVersion#V3} — the lowest version ScyllaDB negotiates, and the reference row for
   * every version above it. Without the pinned version this would report the v3 limit for a cluster
   * deliberately pinned to v2, whose pools are sized from the v1 row instead.
   *
   * <p>That fallback is an assumption rather than an observation, and it is the one way this field
   * can be wrong: an <em>unpinned</em> cluster negotiates downward from the highest version the
   * driver supports, so one that settles on v2 — a Cassandra 2.0 cluster, no ScyllaDB being that
   * old — is sized from the v1 row (128) while this has already reported 1024. Pinning is the only
   * part of negotiation knowable before the control connection is up, and the report is built once,
   * so there is nothing later to correct it with.
   *
   * <p>Looked up here rather than in a static field so that a failure stays inside {@link
   * #buildReport()}'s fail-safe handling instead of breaking class initialization on the {@link
   * Connection.Factory} path.
   */
  private int poolDefault(String key) {
    ProtocolVersion pinned = configuration.getProtocolOptions().initialProtocolVersion;
    ProtocolVersion version = pinned == null ? ProtocolVersion.V3 : pinned;
    ProtocolVersion reference = null;
    for (ProtocolVersion candidate : PoolingOptions.DEFAULTS.keySet()) {
      if (candidate.compareTo(version) > 0) {
        break;
      }
      reference = candidate;
    }
    // V1 is a key, so it is always at or below any version; a null would only mean DEFAULTS lost
    // it,
    // and the NPE is then contained by buildReport()'s fail-safe.
    return PoolingOptions.DEFAULTS.get(reference).get(key);
  }

  /**
   * {@code value} unless it is {@link PoolingOptions#UNSET}, in which case {@code defaultValue}.
   *
   * <p>Only {@code UNSET} falls back: {@link
   * PoolingOptions#setMaxRequestsPerConnection(HostDistance, int)} accepts 0, and reporting the
   * protocol default for it would misreport a limit the operator set deliberately. 0 is below the
   * schema's minimum and reported as-is — see the class javadoc.
   */
  private static int effective(int value, int defaultValue) {
    return value == PoolingOptions.UNSET ? defaultValue : value;
  }
}
