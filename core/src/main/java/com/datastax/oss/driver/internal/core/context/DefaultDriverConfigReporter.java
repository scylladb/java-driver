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

import com.datastax.dse.driver.internal.core.loadbalancing.DseDcInferringLoadBalancingPolicy;
import com.datastax.dse.driver.internal.core.loadbalancing.DseLoadBalancingPolicy;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.channel.Channel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link DriverConfigReporter}: serializes the driver configuration to the cross-driver
 * {@code DRIVER_CONFIG} JSON shape and adds it to the control connection's {@code STARTUP} options.
 *
 * <p><b>The rule this class is built on: report the object that is in force, not the configuration
 * it was built from.</b> The blob is (re)built on demand every time the control connection
 * initializes, so a group whose option is re-read on every use reads the current (possibly
 * reloaded) {@link DriverExecutionProfile} here too. But a policy or factory that captures an
 * option once — into a {@code final} field in its constructor, or at {@code init()} — keeps using
 * that value for the life of the session, because the context holds every one of them in a
 * once-built {@code LazyReference}. For those the profile is the wrong source: after a reload it
 * carries values no request executes with, and {@code advanced.speculative-execution-policy} is
 * documented as not modifiable at runtime outright. So they are read off the running instance —
 * {@code connection.reconnection.policy}, {@code query.speculative-execution.policy}, {@code
 * query.load-balancing.policy.adaptive-ordering}, the first term of {@code
 * fallback-to-non-preferred-nodes}, both {@code node-preference} groups, and {@code
 * connection.tls.hostname-verification} off the active handler factory rather than the configured
 * engine factory.
 *
 * <p>Anything added here owes the same question: find what consumes the option, and if it stores
 * the value rather than re-reading it, expose an accessor and read that. Every field that got this
 * wrong reported a plausible value that no traffic was subject to, which is the one failure mode a
 * diagnostic cannot afford.
 *
 * <p>No profile read can cost more than the field it describes. {@code reference.conf} ships inside
 * the driver jar and {@code OptionsMap.driverDefaults()} fills every option this class touches, so
 * a missing one takes a config source that supplies neither — but the no-fallback getters throw
 * when an option is absent, and one throw here used to drop the whole report behind a single
 * warning. So every read is either guarded by {@link DriverExecutionProfile#isDefined} or passes an
 * explicit fallback, chosen by what the schema permits: where the field or its group is
 * <em>optional</em>, the fallback is the same "disabled" sentinel that already omits it (an
 * undefined page size reads as unbounded, an undefined timeout as off), and where the field is
 * <em>required</em> the fallback is the value {@code reference.conf} documents, since dropping it
 * would make the whole document invalid. Two of the required ones cannot be reached in practice:
 * {@code ChannelFactory} reads {@code max-requests-per-connection} while building the channel and
 * the built-in load balancing policies resolve {@code basic.request.consistency} in their
 * constructor, so a session missing either never gets far enough to report anything.
 *
 * <p>Follows the schema's omission principle throughout: a key the Java driver has no equivalent
 * for is left out of the JSON entirely rather than reported as {@code null}. The same applies where
 * a configured value falls outside what the schema can express but the key is <em>optional</em>: a
 * disabled request timeout, a disabled {@code SO_LINGER}, an unbounded page size, a {@code
 * basic.request.serial-consistency} outside the schema's two serial levels and the like are omitted
 * rather than emitted as a value the schema rejects. Two optional booleans are omitted for a third
 * reason — the answer is genuinely unknown, which is the only thing the schema lets their absence
 * mean: {@code connection.tls.hostname-verification} when the SSL handler or engine in force does
 * not expose it, and {@code query.defaults.client-timestamps} when the timestamp generator is not
 * one this class recognizes (see {@link #tls} and {@link #clientTimestamps}). Guessing a boolean
 * there would describe a security control, or a write-timestamp source, that may well be the
 * opposite — which is also why neither is asked of the SPI itself: an accessor on either SPI would
 * have needed a default, and a default answer is exactly the guess being avoided.
 *
 * <p><b>A new field owes three checks</b>, each of which this class has already got wrong once and
 * each of which is cheap to run before review does it for you:
 *
 * <ol>
 *   <li><b>Source.</b> The rule above — is the value re-read on every use, or captured once?
 *   <li><b>Range.</b> Every value the option legally accepts must land inside the schema's
 *       constraint or take the omission route: a disabled or zero setting, a negative one, a
 *       sub-millisecond duration against a field counting whole milliseconds, and the option being
 *       undefined altogether. Note the driver's units and the schema's need not agree — {@code
 *       max-executions} counts the initial execution and the schema does not — so compare the two
 *       definitions rather than the two names, and pin each boundary with a test.
 *   <li><b>Warrant.</b> Do not assert a property the implementation does not guarantee. The line
 *       this class draws: nothing is <em>inferred</em> on a third party's behalf — no {@code
 *       dc-auto} for a policy the SPI never obliged to work one out, no guessed boolean for a
 *       component this class does not recognize — but what the operator explicitly
 *       <em>configured</em> is passed through even where the component in force may ignore it,
 *       because hiding a real setting is the worse failure. Where that line lands is a judgement
 *       call; say which side and why.
 * </ol>
 *
 * <p><b>Known limitation:</b> that omission is not always available. Two <em>required</em> fields
 * are constrained slightly more tightly than the driver option behind them, so a configured value
 * can in principle fall outside what the schema admits. Only the first is reachable through a live
 * session:
 *
 * <ul>
 *   <li>{@code query.defaults.consistency} is a closed enum of the levels the schema knows, while
 *       {@code basic.request.consistency} is an unvalidated string. A name outside that enum fails
 *       the session as long as the load balancing policy is a built-in one, since those resolve it
 *       through the {@code ConsistencyLevelRegistry} in their constructor — so reaching this needs
 *       a custom registry that defines extra names, or a custom policy that never resolves the
 *       default (see {@link #queryDefaults}). Its optional sibling {@code
 *       query.defaults.serial-consistency} has the same mismatch and is <em>not</em> in this list,
 *       precisely because being optional lets it take the omission route instead.
 *   <li>{@code connection.requests.in-flight.max} must be strictly positive, and nothing validates
 *       {@code advanced.connection.max-requests-per-connection} against that: {@code
 *       ChannelFactory} hands the configured value straight to {@code StreamIdGenerator}, which
 *       does not range-check it (see {@link #requests}). No live session can report such a value
 *       all the same, because it is the connection that fails first: a negative setting makes
 *       {@code StreamIdGenerator}'s {@code BitSet} throw while the channel is still being built,
 *       and zero leaves no stream id for the control connection's own {@code OPTIONS} — {@code
 *       ChannelHandlerRequest} fails it on {@code preAcquireId} before this class is ever asked for
 *       a report. So the mismatch is unreachable by construction rather than a live exposure; the
 *       value is nonetheless reported as configured, and pinned by a test, so that the behavior is
 *       defined if the driver ever stops failing that early. The same setting would also drive
 *       {@code connection.requests.orphaned.max} negative (see {@code
 *       ChannelFactory#effectiveMaxOrphanRequests}), which is the second reason not to read this as
 *       one field's gap.
 * </ul>
 *
 * <p>Both values are reported <em>as-is</em>. The reporter deliberately neither fabricates an
 * admissible value — which would misreport a setting an operator may have chosen on purpose — nor
 * drops the whole report over one field, so such a document is accurate but fails schema
 * validation. For the consistency level, that is a cross-driver schema gap: the fix is to let the
 * field express the value, the way {@code control-plane.schema.agreement.timeout-ms} already admits
 * 0, the constant policy delays do, and {@code query.defaults.request} now does by being optional.
 *
 * <p>Where a duration <em>is</em> reported, any strictly positive value reports as at least
 * 1&nbsp;millisecond (see {@link #positiveMillis}), so a sub-millisecond timeout that is live at
 * runtime is never reported as the 0 that means "disabled". Three fields are deliberately exempt,
 * because 0 is what they really mean there: {@code connection.connect.timeout-ms} (Netty's {@code
 * CONNECT_TIMEOUT_MILLIS} takes the truncated millisecond value, where 0 disables the timeout),
 * {@code control-plane.queries.system.timeout.server-side-ms} (its value goes on the wire as the
 * millisecond argument of a {@code USING TIMEOUT} clause, so a sub-millisecond setting really is
 * {@code 0ms} server-side), and {@code query.speculative-execution.policy.delay-ms} (which is the
 * policy's own already-truncated millisecond count, and whose {@code reference.conf} documents
 * delays below 1 millisecond as equivalent to 0).
 *
 * <p><b>Known limitation:</b> the report always describes {@link
 * com.datastax.oss.driver.api.core.config.DriverExecutionProfile#DEFAULT_NAME the default execution
 * profile}, not whichever profile a given request actually runs with. A session that relies on
 * named execution profiles for some of its traffic will have that traffic's real settings
 * (consistency level, timeouts, retry policy, ...) differ from what {@code DRIVER_CONFIG} reports.
 * Reporting per-profile configuration would need a schema shape for multiple profiles, which the
 * cross-driver schema doesn't define; this is a known gap, not an oversight.
 *
 * <p><b>Thread safety:</b> this class is safe to use as shipped, and holds no mutable state. Note
 * that {@code buildJson(Channel)} runs on every control-connection (re)initialization, and may be
 * called concurrently with a reconnect racing a fresh session start.
 */
@ThreadSafe
public class DefaultDriverConfigReporter implements DriverConfigReporter {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDriverConfigReporter.class);

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
   * <p>{@code STARTUP} options are serialized with {@code PrimitiveCodec#writeString(String,
   * Object)}, which writes a 16-bit length prefix with no bounds check (see {@code
   * ByteBufPrimitiveCodec}): a value longer than 65535 bytes would silently truncate that prefix
   * modulo 65536 while still appending the whole body, corrupting the frame and failing the
   * handshake. Most of this report is fixed-shape, but some of it is user-supplied and unbounded —
   * datacenter and rack names, consistency levels, and the class names of custom policy objects —
   * so enforcing a limit here keeps "reporting must never prevent a connection from being
   * established" a property of this class rather than of the user's configuration. 32KiB is
   * generous for a configuration report, and is the same limit the other ScyllaDB drivers apply.
   */
  static final int MAX_DRIVER_CONFIG_LENGTH = 32 * 1024;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final InternalDriverContext context;

  public DefaultDriverConfigReporter(InternalDriverContext context) {
    this.context = context;
  }

  @Override
  public void populateControlConnectionOptions(
      @NonNull Map<String, String> startupOptions, @NonNull Channel channel) {
    // Configuration reporting is a best-effort diagnostic aid: it runs on the connection
    // initialization path, so any failure here (a bad config read, a misbehaving policy while
    // introspecting, a serialization error) must be swallowed rather than allowed to break the
    // connection — which would prevent the session from establishing or reconnecting.
    //
    // RuntimeException only, deliberately. The one Error this class knows how to provoke is the
    // InternalError that getClass().getSimpleName() throws for certain synthetic classes, and that
    // is caught where it happens (see #policyName). Catching it here as well would also swallow an
    // InternalError raised by anything else on this path — config access, a user policy, Jackson —
    // and an InternalError is a VirtualMachineError: masking one hides a real JVM-level failure
    // behind a diagnostic that was never meant to be load-bearing.
    try {
      if (!isEnabled()) {
        return;
      }
      String json = buildJson(channel);
      if (json == null) {
        return;
      }
      // Measured on the encoded bytes, since that is what the length prefix on the wire counts.
      int length = json.getBytes(StandardCharsets.UTF_8).length;
      if (length > MAX_DRIVER_CONFIG_LENGTH) {
        LOG.warn(
            "The driver configuration report is {} bytes long, which exceeds the {} byte limit; "
                + "skipping DRIVER_CONFIG",
            length,
            MAX_DRIVER_CONFIG_LENGTH);
        return;
      }
      startupOptions.put(DRIVER_CONFIG_KEY, json);
    } catch (RuntimeException e) {
      LOG.warn("Error while building the driver configuration report; skipping DRIVER_CONFIG", e);
    }
  }

  // Read on every control-connection initialization rather than cached, so that a configuration
  // reload takes effect on the next (re)connect. The fallback mirrors the reference.conf default,
  // so that a configuration omitting the option behaves like the shipped one.
  private boolean isEnabled() {
    return context
        .getConfig()
        .getDefaultProfile()
        .getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, true);
  }

  /**
   * Builds the compact, single-line JSON configuration report.
   *
   * <p>Relies on the policy/generator {@code LazyReference}s (reconnection, retry, speculative
   * execution, load balancing, timestamp generator, SSL handler factory) already being resolved by
   * the time this runs, which holds because {@code DefaultSession}'s init eagerly forces all of
   * them — and this reporter itself — before any connection is opened, so no reference is first
   * resolved from a Netty event-loop thread mid-{@code STARTUP} build. That ordering isn't this
   * class's to enforce: a future change to session bootstrap that dropped one of those from the
   * eager list would quietly reintroduce that.
   *
   * <p>The configured SSL <em>engine</em> factory is deliberately not among them: {@link
   * #tls(Channel)} reads state recorded for the control channel by the {@code JdkSslHandlerFactory}
   * in force rather than resolving the factory behind {@code getSslEngineFactory()}. Those can
   * differ — a context that overrides {@code buildSslHandlerFactory()} may wrap an engine factory
   * of its own — and going through the context would both describe an engine nothing on the
   * connection path uses and risk being the first caller to resolve it, which for the built-in
   * factory means reading keystore/truststore files on a Netty event-loop thread (and failing the
   * whole report if that throws).
   *
   * @return the report, or {@code null} if it could not be serialized — in which case {@code
   *     DRIVER_CONFIG} is skipped rather than the connection failed.
   */
  @Nullable
  String buildJson(Channel channel) {
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    root.put("version", SCHEMA_VERSION);
    populateConfig(root, context.getConfig().getDefaultProfile(), channel);
    try {
      return OBJECT_MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      // An in-memory node tree should never fail to serialize; never let it break connection setup.
      LOG.warn("Failed to serialize driver configuration report; skipping DRIVER_CONFIG", e);
      return null;
    }
  }

  /**
   * Populates the configuration groups onto the report root, from the default execution profile
   * plus the context's policies. Each group follows the cross-driver schema; a key the Java driver
   * has no equivalent for is omitted rather than reported as {@code null}.
   */
  private void populateConfig(ObjectNode root, DriverExecutionProfile config, Channel channel) {
    // Resolved once and shared: the load balancing policy decides both its own group and the
    // node-location preferences reported under two different parents, and resolving it twice would
    // mean a second SPI lookup on the Netty event-loop thread that is building STARTUP.
    LoadBalancingPolicy loadBalancingPolicy =
        context.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);
    NodeLocation nodeLocation = nodeLocation(config, loadBalancingPolicy);
    root.set("connection", connection(config, nodeLocation, channel));
    root.set("control-plane", controlPlane(config));
    root.set("query", query(config, loadBalancingPolicy, nodeLocation));
  }

  /**
   * Everything the driver applies per connection: the socket beneath it, the CQL-level settings on
   * top of it, how it is re-established, and which part of the cluster gets one at all.
   */
  private ObjectNode connection(
      DriverExecutionProfile config, @Nullable NodeLocation nodeLocation, Channel channel) {
    ObjectNode n = connectionTimeouts(config);
    n.set("socket", socket(config));
    ObjectNode reconnection = OBJECT_MAPPER.createObjectNode();
    reconnection.set("policy", reconnectionPolicy());
    n.set("reconnection", reconnection);
    // Optional, and absent rather than false when off: presence of the group is what says TLS is
    // enabled, since the schema dropped the boolean that used to carry it.
    ObjectNode tls = tls(channel);
    if (tls != null) {
      n.set("tls", tls);
    }
    // The datacenter half only. Java's local rack never reaches computeNodeDistance() — it only
    // reorders replicas at the head of a query plan (see DefaultLoadBalancingPolicy
    // #shuffleLocalRackReplicasAndReplicas) — so connections are still held across the whole local
    // DC, and reporting the rack here would claim a scoping the driver does not perform. The
    // datacenter, on the other hand, genuinely does scope pooling: a node outside it is IGNORED,
    // and an IGNORED node gets no pool.
    if (nodeLocation != null) {
      n.set("node-preference", nodeLocation.toDatacenterPreference());
    }
    return n;
  }

  private ObjectNode connectionTimeouts(DriverExecutionProfile config) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    ObjectNode connect = OBJECT_MAPPER.createObjectNode();
    // The enclosing "connect" object is required, its "timeout-ms" is not: DefaultNettyOptions
    // hands the truncated millisecond value to Netty's CONNECT_TIMEOUT_MILLIS, where 0 means "no
    // connect timeout", which the schema's positive-only field cannot express — so a disabled
    // timeout is omitted rather than reported as a 0 the schema rejects. Deliberately measured on
    // the emitted milliseconds, not the Duration, because Netty truncates the same way: a
    // sub-millisecond connect timeout genuinely is disabled at runtime.
    long connectTimeoutMs =
        config
            .getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ZERO)
            .toMillis();
    if (connectTimeoutMs > 0) {
      connect.put("timeout-ms", connectTimeoutMs);
    }
    n.set("connect", connect);
    n.set("requests", requests(config));
    ObjectNode pool = OBJECT_MAPPER.createObjectNode();
    ObjectNode shardAware = OBJECT_MAPPER.createObjectNode();
    shardAware.put(
        "enabled",
        config.getBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED, true));
    pool.set("shard-aware", shardAware);
    n.set("pool", pool);
    // The Java driver has no socket-level read/write timeouts, and connection.heartbeat is a
    // reserved empty placeholder in this schema version (no slot for HEARTBEAT_INTERVAL/TIMEOUT
    // yet) — all three are omitted entirely rather than reported as empty/null.
    return n;
  }

  /** Per-connection request capacity: the in-flight ceiling and the orphaned-request threshold. */
  private ObjectNode requests(DriverExecutionProfile config) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    int maxRequests = config.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 1024);
    ObjectNode inFlight = OBJECT_MAPPER.createObjectNode();
    // Reported as-is. reference.conf documents this option as "strictly positive, and less than
    // 32768", but that is documentation only: ChannelFactory hands the configured value to
    // StreamIdGenerator, which does not range-check it. The schema dropped the upper half of that
    // bound and now only requires a positive integer, so the only value it would reject is a
    // non-positive one — which no live session can reach, since the connection fails before any
    // report is built (see the class javadoc). Reported rather than clamped to a limit that no
    // connection was built with, so the behavior stays defined either way.
    inFlight.put("max", maxRequests);
    n.set("in-flight", inFlight);
    ObjectNode orphaned = OBJECT_MAPPER.createObjectNode();
    // The effective threshold, not the configured one: ChannelFactory silently replaces a value
    // that isn't below max-requests-per-connection with a quarter of it, so reporting the raw
    // option would describe a limit no connection was built with.
    orphaned.put(
        "max",
        ChannelFactory.effectiveMaxOrphanRequests(
            maxRequests, config.getInt(DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, 256)));
    n.set("orphaned", orphaned);
    return n;
  }

  private ObjectNode socket(DriverExecutionProfile config) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    n.put("tcp-no-delay", config.getBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY, true));
    // keep-alive and reuse-address are unset by default; the driver leaves the socket option
    // untouched, so the effective value is the JDK/OS default — false for both SO_KEEPALIVE and
    // (client-socket) SO_REUSEADDR. The schema requires both keys, so they are always emitted.
    n.put("keep-alive", config.getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, false));
    n.put("reuse-address", config.getBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS, false));
    // A negative linger interval means SO_LINGER is disabled (reference.conf documents the
    // sentinel), which the schema's non-negative interval-s cannot express — so the group is
    // omitted in that case, the same way "page" is when paging is unbounded. Zero is a real
    // value here (close immediately) and is reported.
    if (config.isDefined(DefaultDriverOption.SOCKET_LINGER_INTERVAL)) {
      int lingerInterval = config.getInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL);
      if (lingerInterval >= 0) {
        ObjectNode linger = OBJECT_MAPPER.createObjectNode();
        linger.put("interval-s", lingerInterval);
        n.set("linger", linger);
      }
    }
    // Both buffer sizes are positive-only in the schema, and a non-positive one wouldn't survive
    // Netty's own validation anyway; omit rather than emit a value the schema rejects.
    if (config.isDefined(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE)) {
      int size = config.getInt(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE);
      if (size > 0) {
        ObjectNode receiveBuffer = OBJECT_MAPPER.createObjectNode();
        receiveBuffer.put("size-bytes", size);
        n.set("receive-buffer", receiveBuffer);
      }
    }
    if (config.isDefined(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE)) {
      int size = config.getInt(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE);
      if (size > 0) {
        ObjectNode sendBuffer = OBJECT_MAPPER.createObjectNode();
        sendBuffer.put("size-bytes", size);
        n.set("send-buffer", sendBuffer);
      }
    }
    return n;
  }

  private ObjectNode controlPlane(DriverExecutionProfile config) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();

    // These two are siblings under one "timeout" object but are NOT two views of the same timeout:
    // client-side-ms is CONTROL_CONNECTION_TIMEOUT, which bounds topology and schema-agreement
    // polling, while server-side-ms is METADATA_SCHEMA_REQUEST_TIMEOUT, which bounds schema queries
    // (and is also their own client-side wait, see CassandraSchemaQueries). So no single query is
    // subject to both numbers. That grouping is a characteristic of the cross-driver schema, not a
    // choice made here — the mapping matches the schema's own per-driver table.
    //
    // Open item for the schema owner, with a concrete shape now that "system" is one key under
    // "queries": a sibling would let each class of control query carry an honest pair, i.e.
    // queries.system.timeout.client-side-ms <- CONTROL_CONNECTION_TIMEOUT (no server side) and
    // queries.schema.timeout.{client-side-ms,server-side-ms} <- METADATA_SCHEMA_REQUEST_TIMEOUT.
    // Not emitted here: "queries" is additionalProperties:false, so a "schema" sibling would fail
    // validation until the spec adds it.
    ObjectNode timeout = OBJECT_MAPPER.createObjectNode();
    // Both fields are optional and positive-only, and both options treat a non-positive value
    // as "no timeout"; omit rather than report a 0 the schema rejects. AdminRequestHandler
    // schedules this one in nanoseconds, so a sub-millisecond value is a live timeout and rounds
    // up to 1 rather than being mistaken for a disabled one.
    long clientSideMs =
        positiveMillis(
            config.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ZERO));
    if (clientSideMs > 0) {
      timeout.put("client-side-ms", clientSideMs);
    }
    // Reported as configuration intent, not as an observed effect, and therefore independent of the
    // backend: CassandraSchemaQueries adds a "USING TIMEOUT <ms>ms" clause built from this value to
    // every schema query, but only where sharding information says the peer is ScyllaDB — on
    // genuine Cassandra the clause never goes on the wire and the option acts as a client-side wait
    // alone. Gating the field on that check described the effect more precisely but made the report
    // depend on peer detection for a value the driver knows before it connects; per the schema
    // owner the field carries what is configured, the way pool.shard-aware.enabled already does.
    //
    // Deliberately not floored to 1 like the timeouts above: the value goes on the wire as a whole
    // millisecond argument, so a sub-millisecond setting really is 0ms server-side.
    long serverSideMs =
        config
            .getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ZERO)
            .toMillis();
    if (serverSideMs > 0) {
      timeout.put("server-side-ms", serverSideMs);
    }
    ObjectNode system = OBJECT_MAPPER.createObjectNode();
    system.set("timeout", timeout);
    ObjectNode queries = OBJECT_MAPPER.createObjectNode();
    queries.set("system", system);
    n.set("queries", queries);

    ObjectNode schemaAgreement = OBJECT_MAPPER.createObjectNode();
    // Required and non-negative in the schema, where 0 specifically means "do not wait" — which
    // matches SchemaAgreementChecker skipping the check entirely at 0. A negative value behaves
    // identically (the first pass is already past the deadline), so normalizing it to 0 is exact
    // rather than invented. The checker holds this timeout in nanoseconds, so a positive
    // sub-millisecond value does wait, and must not collapse onto that "do not wait" 0.
    schemaAgreement.put(
        "timeout-ms",
        positiveMillis(
            config.getDuration(
                DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(10))));
    ObjectNode schema = OBJECT_MAPPER.createObjectNode();
    schema.set("agreement", schemaAgreement);
    n.set("schema", schema);

    return n;
  }

  /**
   * Everything that governs how a statement is executed: the per-request defaults, and the three
   * policies that decide where it goes, whether it is retried, and whether it is raced.
   */
  private ObjectNode query(
      DriverExecutionProfile config,
      LoadBalancingPolicy policy,
      @Nullable NodeLocation nodeLocation) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    n.set("defaults", queryDefaults(config));

    ObjectNode retry = OBJECT_MAPPER.createObjectNode();
    retry.set("policy", retryPolicy());
    // "backoff" is a sibling of "policy" rather than a field on it, but no built-in Java retry
    // policy inserts a delay between attempts and none takes one from configuration, so there is
    // nothing to report and the optional key is omitted.
    n.set("retry", retry);

    ObjectNode loadBalancing = OBJECT_MAPPER.createObjectNode();
    loadBalancing.set("policy", loadBalancingPolicy(policy, nodeLocation));
    // The full preference, rack included: unlike connection scoping, query routing is exactly what
    // the rack affects.
    if (nodeLocation != null) {
      loadBalancing.set("node-preference", nodeLocation.toFullPreference());
    }
    n.set("load-balancing", loadBalancing);

    // Optional group, and the policy inside it is required — so when there is no speculative
    // execution to describe the whole group goes, rather than an empty object.
    ObjectNode specExec = speculativeExecutionPolicy();
    if (specExec != null) {
      ObjectNode speculativeExecution = OBJECT_MAPPER.createObjectNode();
      speculativeExecution.set("policy", specExec);
      n.set("speculative-execution", speculativeExecution);
    }
    return n;
  }

  /**
   * The reconnection policy, read from the <em>running instance</em> rather than from the profile.
   *
   * <p>Both built-ins latch their delays into final fields at construction and never re-read them,
   * so after a configuration reload the profile describes a policy that is not the one reconnecting
   * — the instance is the only accurate source. It also makes the schema's {@code max-ms >=
   * base-ms} invariant hold for free, since {@code ExponentialReconnectionPolicy} enforces it in
   * its constructor whereas a reloaded profile could carry any pair.
   */
  private ObjectNode reconnectionPolicy() {
    ReconnectionPolicy policy = context.getReconnectionPolicy();
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // Exact-class checks, not instanceof: none of these built-ins are final, so a user subclass
    // (e.g. to tweak one method) must fall through to the "custom" branch below rather than be
    // misreported as the unmodified built-in.
    if (policy.getClass() == ExponentialReconnectionPolicy.class) {
      ExponentialReconnectionPolicy exponential = (ExponentialReconnectionPolicy) policy;
      n.put("type", "exponential");
      n.put("base-ms", exponential.getBaseDelayMs());
      n.put("max-ms", exponential.getMaxDelayMs());
      // Java's built-in reconnection policies are unbounded: max-attempts is omitted.
    } else if (policy.getClass() == ConstantReconnectionPolicy.class) {
      n.put("type", "constant");
      // Non-negative in the schema, where 0 specifically means "reconnect immediately".
      // ConstantReconnectionPolicy rejects only a negative delay, so a sub-millisecond one is legal
      // — and live, since Reconnection schedules nextDelay() in nanoseconds — which is why it
      // floors
      // at 1 rather than truncating onto that "immediately" 0.
      n.put("delay-ms", positiveMillis(((ConstantReconnectionPolicy) policy).getDelay()));
    } else {
      customPolicy(n, policy);
    }
    return n;
  }

  /**
   * The retry policy. Neither built-in fills the schema's optional {@code max-retries}: the key
   * reports a <em>configured</em> retry limit, and Java has no such option. What both policies have
   * instead are per-error-type rules hardcoded in Java — a single attempt for read timeouts, write
   * timeouts and unavailable, but an unbounded walk down the query plan for aborted requests and
   * error responses — which no single number describes.
   *
   * <p>Worth spelling out, because {@code 1} looks like the right answer and is not. {@code
   * DefaultRetryPolicy} gates {@code onReadTimeout}, {@code onWriteTimeout} and {@code
   * onUnavailable} on {@code retryCount == 0}, so those three really are bounded at one. The error
   * paths are not: {@code CqlRequestHandler} consults {@code onErrorResponseVerdict} only when
   * {@code Conversions.resolveIdempotence} says the statement is idempotent, and then never checks
   * the count — so the very same policy bounds a non-idempotent request at 1 and an idempotent one
   * at the length of the query plan. Idempotence is chosen per statement, which a session-level
   * report cannot know, so there is no honest number to publish and the key is omitted. Settled the
   * same way on the 3.x port (scylladb/java-driver#974); restoring the cap that
   * scylladb/java-driver#992 tracks would make {@code 1} unconditional and this reportable.
   */
  private ObjectNode retryPolicy() {
    RetryPolicy policy = context.getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME);
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // Exact-class check, not instanceof: DefaultRetryPolicy/ConsistencyDowngradingRetryPolicy are
    // not final, so a user subclass must fall through to "custom" rather than be misreported.
    if (policy.getClass() == DefaultRetryPolicy.class) {
      n.put("type", "standard-error-aware");
      // No configurable backoff and no configured retry limit: both omitted.
    } else if (policy.getClass() == ConsistencyDowngradingRetryPolicy.class) {
      n.put("type", "downgrading-consistency");
      // No configurable backoff and no configured retry limit: both omitted.
    } else {
      customPolicy(n, policy);
    }
    return n;
  }

  /**
   * Returns {@code null} when there is no speculative execution policy to report, in which case the
   * whole group is omitted from the report (the schema has no null variant for it).
   *
   * <p>Both values are read off the policy instance rather than the profile, for the reason spelled
   * out in {@link #reconnectionPolicy()}: the policy captured them at construction and {@code
   * advanced.speculative-execution-policy} is documented as not modifiable at runtime, so a
   * reloaded profile can carry numbers no request is actually executed with — and, unlike the
   * policy's constructor, admits values this field's schema range rejects.
   */
  private ObjectNode speculativeExecutionPolicy() {
    SpeculativeExecutionPolicy policy =
        context.getSpeculativeExecutionPolicy(DriverExecutionProfile.DEFAULT_NAME);
    // Exact-class checks, not instanceof: neither built-in is final.
    if (policy.getClass() == NoSpeculativeExecutionPolicy.class) {
      return null;
    }
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    if (policy.getClass() == ConstantSpeculativeExecutionPolicy.class) {
      ConstantSpeculativeExecutionPolicy constant = (ConstantSpeculativeExecutionPolicy) policy;
      // The policy counts the initial, non-speculative execution as one of the executions it caps
      // (see SpeculativeExecutionPolicy#nextExecution's runningExecutions), while the schema field
      // counts only the speculative ones — hence the -1. Its constructor rejects anything below 1,
      // so the result cannot be negative; 1 means the policy never speculates, which is reported
      // the way NoSpeculativeExecutionPolicy is — by omitting the group, since there is nothing to
      // describe and the schema field is positive-only.
      int speculativeExecutions = constant.getMaxExecutions() - 1;
      if (speculativeExecutions < 1) {
        return null;
      }
      n.put("type", "constant");
      n.put("max-executions", speculativeExecutions);
      // No sub-millisecond value survives to floor here, unlike the timeouts: the policy already
      // holds whole milliseconds, and reference.conf documents delays of less than 1 millisecond as
      // equivalent to 0 for this option — which the schema accepts ("launch immediately").
      n.put("delay-ms", constant.getConstantDelayMillis());
    } else {
      customPolicy(n, policy);
    }
    return n;
  }

  private ObjectNode loadBalancingPolicy(
      LoadBalancingPolicy policy, @Nullable NodeLocation nodeLocation) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // The schema's built-in variant is a single type: every policy the driver ships routes queries
    // to token replicas, so all four report "token-aware", and what distinguishes them is carried
    // by the fields below (and, for the local DC, by node-location-preference). Exact-class checks
    // (not instanceof) so an actual user subclass of any of them still falls through to "custom".
    //
    // DcInferringLoadBalancingPolicy extends DefaultLoadBalancingPolicy, overriding only how the
    // local DC is discovered; DseLoadBalancingPolicy/DseDcInferringLoadBalancingPolicy are
    // deprecated, behavior-identical aliases of the two ("equivalent to DefaultLoadBalancingPolicy,
    // which should now be used instead" per their own javadoc). All of them extend
    // BasicLoadBalancingPolicy, from which they inherit an unconditional shuffle of the replica
    // head
    // and the same DC-failover option; only slow-replica avoidance differs, so it is resolved per
    // class below and the shared fields are written once.
    Class<?> policyClass = policy.getClass();
    boolean avoidsSlowReplicas;
    if (policyClass == DefaultLoadBalancingPolicy.class
        || policyClass == DcInferringLoadBalancingPolicy.class
        || policyClass == DseLoadBalancingPolicy.class
        || policyClass == DseDcInferringLoadBalancingPolicy.class) {
      // Off the running instance, not the profile: DefaultLoadBalancingPolicy latches this flag in
      // its constructor and never re-reads it, so after a configuration reload the profile can say
      // one thing while the policy keeps reordering (or not) according to the other. Same reason
      // the reconnection delays and the speculative-execution parameters are read off their
      // policies.
      avoidsSlowReplicas = ((DefaultLoadBalancingPolicy) policy).isAvoidingSlowReplicas();
    } else if (policyClass == BasicLoadBalancingPolicy.class) {
      // Unlike DefaultLoadBalancingPolicy, BasicLoadBalancingPolicy has no slow-replica-avoidance
      // mechanism at all.
      avoidsSlowReplicas = false;
    } else {
      customPolicy(n, policy);
      return n;
    }
    n.put("type", "token-aware");
    // BasicLoadBalancingPolicy#shuffleHead shuffles the replicas at the head of every query plan
    // whenever there is more than one, and no built-in policy has an option to disable that — so
    // "shuffle" rather than "round-robin" (which the driver applies only to the non-replica tail)
    // or
    // "replica-set" (which would mean leaving the replica order untouched).
    n.put("load-distribution", "shuffle");
    // Both terms are necessary: BasicLoadBalancingPolicy#maybeAddDcFailover appends remote nodes to
    // a query plan only when max-nodes-per-remote-dc is positive AND the policy has a local DC to
    // treat as preferred. Reading the option alone reported failover as on for a config that
    // changed
    // nothing but that option, where no remote node is ever added — and the key's own definition is
    // about leaving the preference, so with no preference there is nothing to leave. That is
    // exactly
    // what a null nodeLocation means here, so the predicate is reused rather than restated.
    //
    // Still an approximation in one respect, and deliberately: maybeAddDcFailover also consults
    // isDcFailoverAllowedForRequest, which is false for a DC-local consistency while
    // allow-for-local-consistency-levels is off. That is a per-request decision a statement can
    // change, and re-deriving it here would duplicate policy logic in a diagnostic.
    //
    // The first term comes off the running instance for the same reason as adaptive ordering above:
    // BasicLoadBalancingPolicy latches max-nodes-per-remote-dc in its constructor. Every class that
    // reaches this point is one of the built-ins, so the cast holds.
    boolean dcFailoverConfigured = ((BasicLoadBalancingPolicy) policy).getMaxNodesPerRemoteDc() > 0;
    n.put("fallback-to-non-preferred-nodes", dcFailoverConfigured && nodeLocation != null);
    // Optional, and its presence is what says adaptive ordering is on: the schema dropped the
    // boolean that used to carry that and now requires a non-empty signal list, so "off" is the
    // absent group rather than an enabled:false with nothing in it.
    if (avoidsSlowReplicas) {
      n.set("adaptive-ordering", adaptiveOrdering());
    }
    return n;
  }

  /**
   * Which runtime observations reorder candidate nodes. Only called when such reordering is on —
   * the caller omits the whole group otherwise.
   *
   * <p>The driver's only such mechanism is {@code DefaultLoadBalancingPolicy}'s slow-replica
   * avoidance, so the signal list is fixed and describes what {@code avoidSlowReplicas} actually
   * consults: a replica is demoted when it is both busy — {@code getInFlight} at or above the
   * in-flight threshold — and answering too rarely, measured by {@code NodeResponseRateSample}; the
   * first two replicas are then swapped on in-flight count alone; and a replica that came up within
   * the last {@code NEWLY_UP_INTERVAL} is treated specially, so that a node still recovering is not
   * immediately handed the front of the query plan.
   *
   * <p>{@code latency} is deliberately not among them: those samples record <em>when</em> responses
   * arrived, not how long they took, so the driver has no latency-percentile host ordering to
   * report.
   */
  private ObjectNode adaptiveOrdering() {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    ArrayNode signals = n.putArray("signals");
    signals.add("response-rate");
    signals.add("in-flight-requests");
    signals.add("recovery-state");
    return n;
  }

  /**
   * Resolves the session's datacenter/rack preference, or {@code null} when there is none to
   * describe.
   *
   * <p>Java has no session-level locality API separate from the load balancing policy, so the
   * values come from the policy itself wherever it has them: {@code getLocalDatacenter()} and
   * {@code getLocalRack()} hold what it settled on, and that is what {@code computeNodeDistance}
   * and the query plan go by. The policy is a once-built {@code LazyReference}, so a profile
   * reloaded to a different datacenter never reaches it — reading the profile in preference would
   * describe a locality no request is routed by. Before the policy is initialized, which is exactly
   * the state the very first control connection sends {@code STARTUP} in, there is nothing resolved
   * and the configured value stands alone: programmatically via {@link
   * com.datastax.oss.driver.api.core.session.SessionBuilder#withLocalDatacenter} first, then
   * config, mirroring {@code OptionalLocalDcHelper}'s own precedence; the local rack has no
   * programmatic override and is config-only.
   *
   * <p>Whether anything was configured then decides only <em>which</em> of the schema's slots the
   * reported value occupies, configured or inferred — a datacenter the policy took from an earlier
   * generation of the configuration is still explicitly configured, merely stale, and reporting it
   * as inferred would claim the driver worked it out unaided.
   *
   * <p>Returns {@code null} — omitting the group from both of its parents, where it is optional —
   * when the session has no preference to describe: no datacenter configured, none resolved yet,
   * and a policy that is not one of the built-ins {@linkplain #infersLocalDatacenter known to work
   * one out}. {@code dc-auto} is a claim that a datacenter <em>will</em> be settled on, and neither
   * {@code BasicLoadBalancingPolicy} (datacenter-agnostic for the life of the session) nor an
   * arbitrary custom policy (the SPI requires no inference at all) makes that claim good.
   *
   * <p>An <em>explicitly configured</em> datacenter, on the other hand, is reported whatever the
   * policy is — and that is an approximation for a custom one. Both parents describe an effect the
   * built-ins produce: {@link #connection} claims the datacenter scopes which nodes hold a pool
   * (true only because {@code BasicLoadBalancingPolicy#computeNodeDistance} makes an out-of-DC node
   * {@code IGNORED}), and {@link #query} claims it scopes routing. A custom {@link
   * LoadBalancingPolicy} computes distance itself and need not consult either source, so it may
   * honor neither. Reported all the same, deliberately, on the grounds that hiding a setting the
   * operator really did make is the worse failure mode — the same call as reporting a configured
   * rack for a policy that ignores it. Note the asymmetry with the paragraph above: nothing is
   * <em>inferred</em> on a custom policy's behalf, but what was configured is passed through.
   *
   * <p>A configured {@code basic.load-balancing-policy.evaluator.class} weakens the same claim one
   * step further, even for a built-in policy: {@code BasicLoadBalancingPolicy#computeNodeDistance}
   * consults the evaluator before the datacenter, so it can make an in-DC node {@code IGNORED} and
   * leave it without a pool. Nothing is reported for it. The option names a user-supplied class —
   * the driver ships no location-based evaluator of its own, only {@code
   * NodeFilterToDistanceEvaluatorAdapter} around the deprecated {@code filter.class} — so unlike
   * drivers with an introspectable built-in (gocql's {@code DataCenterHostFilter}), there is no
   * datacenter to read out of it, and {@code node-location-preference} has no slot for a class
   * name.
   */
  @Nullable
  private NodeLocation nodeLocation(DriverExecutionProfile config, LoadBalancingPolicy policy) {
    String configuredDc =
        blankToNull(context.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME));
    if (configuredDc == null
        && config.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
      configuredDc =
          blankToNull(config.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER));
    }
    String configuredRack =
        config.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK)
            ? blankToNull(config.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK))
            : null;

    // instanceof here, deliberately unlike the exact-class checks used elsewhere: those decide
    // which built-in we are looking at, and must not be fooled by a subclass. This one only reads a
    // value the policy has already resolved, and a subclass's local DC is every bit as real as the
    // base class's. Both are null until the policy is initialized — which it is not when the very
    // first control connection sends STARTUP, but is on every later reconnect.
    String resolvedDc = null;
    String resolvedRack = null;
    if (policy instanceof BasicLoadBalancingPolicy) {
      BasicLoadBalancingPolicy basic = (BasicLoadBalancingPolicy) policy;
      resolvedDc = blankToNull(basic.getLocalDatacenter());
      resolvedRack = blankToNull(basic.getLocalRack());
    }

    // Once the policy has resolved a value, that is the one reported: it is what
    // computeNodeDistance
    // and the query plan use, and the policy is a once-built LazyReference, so a profile reloaded
    // to
    // a different datacenter never reaches it. The configured value only decides *which slot* the
    // reported one goes in — a datacenter the policy took from an earlier generation of the config
    // is still explicitly configured, just stale, and not something it inferred. Before
    // initialization there is nothing resolved, so the configured value stands on its own; that is
    // the state the very first control connection reports from.
    String localDc = resolvedDc != null ? resolvedDc : configuredDc;
    String localRack = resolvedRack != null ? resolvedRack : configuredRack;
    // Exactly one form of each field survives, which is what keeps the schema's "never a configured
    // and an inferred form of the same field" constraint true by construction.
    //
    // inferredRack stays null for every built-in policy: BasicLoadBalancingPolicy#init discovers
    // the rack through OptionalLocalRackHelper, which reads configuration and never infers one, and
    // only once a datacenter is known — so a resolved rack always implies a configured one. It goes
    // non-null only for a subclass overriding discoverLocalRack, which the instanceof check above
    // deliberately does read.
    String inferredDc = configuredDc == null ? localDc : null;
    String inferredRack = configuredRack == null ? localRack : null;
    configuredDc = configuredDc == null ? null : localDc;
    configuredRack = configuredRack == null ? null : localRack;

    // No datacenter configured and none resolved: report a preference only for a policy that is
    // going to arrive at one. A configured rack alone does not keep the group alive — no built-in
    // looks for a rack before it knows a datacenter (BasicLoadBalancingPolicy#init), so a rack-only
    // configuration is inoperative and describing it would claim a preference that never forms.
    if (configuredDc == null
        && inferredDc == null
        && inferredRack == null
        && !infersLocalDatacenter(policy)) {
      return null;
    }
    return new NodeLocation(configuredDc, configuredRack, inferredDc, inferredRack);
  }

  /**
   * Whether this is one of the built-ins that works a local datacenter out for itself when none is
   * configured — the only case where reporting {@code dc-auto} describes something that is actually
   * going to happen.
   *
   * <p>Exact-class checks, like the policy branches elsewhere, and here for the converse reason:
   * {@link LoadBalancingPolicy} nowhere requires an implementation to infer a datacenter, so
   * neither a custom policy nor a subclass overriding {@code discoverLocalDc} can be assumed to.
   * {@code BasicLoadBalancingPolicy} is deliberately absent — alone among the built-ins it uses
   * {@code OptionalLocalDcHelper}, so it stays datacenter-agnostic for the life of the session. A
   * policy outside this list that <em>does</em> infer one is still reported, once it has: the
   * caller falls back to the datacenter it has already resolved.
   */
  private static boolean infersLocalDatacenter(LoadBalancingPolicy policy) {
    Class<?> policyClass = policy.getClass();
    return policyClass == DefaultLoadBalancingPolicy.class
        || policyClass == DcInferringLoadBalancingPolicy.class
        || policyClass == DseLoadBalancingPolicy.class
        || policyClass == DseDcInferringLoadBalancingPolicy.class;
  }

  /**
   * A resolved datacenter/rack preference, rendered into whichever of the schema's {@code
   * node-location-preference} variants fits what is actually known.
   *
   * <p>The variants encode <em>how</em> the preference was arrived at, not just its value: {@code
   * dc} and {@code rack} are wholly configured, {@code dc-auto} is a datacenter the policy worked
   * out, and {@code rack-auto} covers every mixture ("at least one part is inferred") by carrying
   * configured and inferred fields under distinct names.
   *
   * <p>Only one of those mixtures arises from a built-in policy — a configured rack alongside a
   * datacenter the policy inferred — because none of them ever infers a rack (see {@link
   * #nodeLocation}). The {@code inferred-local-rack} field and the branches that read it are there
   * for a {@code BasicLoadBalancingPolicy} subclass that overrides {@code discoverLocalRack}, whose
   * answer {@link #nodeLocation} does read.
   */
  private static final class NodeLocation {

    @Nullable private final String configuredDc;
    @Nullable private final String configuredRack;
    @Nullable private final String inferredDc;
    @Nullable private final String inferredRack;

    NodeLocation(
        @Nullable String configuredDc,
        @Nullable String configuredRack,
        @Nullable String inferredDc,
        @Nullable String inferredRack) {
      this.configuredDc = configuredDc;
      this.configuredRack = configuredRack;
      this.inferredDc = inferredDc;
      this.inferredRack = inferredRack;
    }

    /**
     * The datacenter half alone, for the preference that scopes which nodes are connected to at
     * all. Never {@code rack}/{@code rack-auto}: the rack is not part of that scoping.
     */
    ObjectNode toDatacenterPreference() {
      ObjectNode n = OBJECT_MAPPER.createObjectNode();
      if (configuredDc != null) {
        n.put("type", "dc");
        n.put("local-dc", configuredDc);
      } else {
        // dc-auto carries the inferred datacenter in plain "local-dc" — unlike rack-auto, which
        // gives it an "inferred-" prefix. That asymmetry is the schema's, not ours.
        n.put("type", "dc-auto");
        if (inferredDc != null) {
          n.put("local-dc", inferredDc);
        }
      }
      return n;
    }

    /** The whole preference, rack included, for the parent that governs query routing. */
    ObjectNode toFullPreference() {
      ObjectNode n = OBJECT_MAPPER.createObjectNode();
      if (configuredDc != null && configuredRack != null) {
        n.put("type", "rack");
        n.put("local-dc", configuredDc);
        n.put("local-rack", configuredRack);
      } else if (configuredDc != null && inferredRack == null) {
        n.put("type", "dc");
        n.put("local-dc", configuredDc);
      } else if (configuredDc == null && configuredRack == null && inferredRack == null) {
        n.put("type", "dc-auto");
        if (inferredDc != null) {
          n.put("local-dc", inferredDc);
        }
      } else {
        // Everything else is a mixture of configured and inferred parts. The schema forbids pairing
        // a configured field with the inferred form of the same field, and forbids carrying an
        // explicit DC together with an explicit rack (that is the "rack" variant above) — the
        // resolver has already made both impossible.
        n.put("type", "rack-auto");
        if (configuredDc != null) {
          n.put("local-dc", configuredDc);
        }
        if (configuredRack != null) {
          n.put("local-rack", configuredRack);
        }
        if (inferredDc != null) {
          n.put("inferred-local-dc", inferredDc);
        }
        if (inferredRack != null) {
          n.put("inferred-local-rack", inferredRack);
        }
      }
      return n;
    }
  }

  private ObjectNode queryDefaults(DriverExecutionProfile config) {
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    int pageSize = config.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 0);
    if (pageSize > 0) {
      ObjectNode page = OBJECT_MAPPER.createObjectNode();
      page.put("size", pageSize);
      n.set("page", page);
    }
    // pageSize <= 0 means paging is unbounded: the "page" group is omitted entirely (the schema
    // has no "unbounded" sentinel).
    // Passed through verbatim: this field is required, so there is no omission route if the
    // configured name is outside the schema's enum. The enum now covers the serial levels as well,
    // and the built-in load balancing policies reject anything the ConsistencyLevelRegistry does
    // not
    // know, so what is left needs a custom registry — see the class javadoc.
    n.put("consistency", config.getString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_ONE"));
    // Both consistency options are unvalidated strings, and both schema fields are closed enums —
    // but this one is optional, so it takes the omission route the required "consistency" above
    // cannot. Nothing checks basic.request.serial-consistency before the first conditional
    // statement runs (Conversions#resolveSerialConsistency), so a session can be started with a
    // value the schema has no member for; reporting it by omission keeps the rest of the document
    // valid, the same way a disabled request timeout does below.
    if (config.isDefined(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY)) {
      String serialConsistency = config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY);
      if ("SERIAL".equals(serialConsistency) || "LOCAL_SERIAL".equals(serialConsistency)) {
        n.put("serial-consistency", serialConsistency);
      }
    }
    n.put("idempotence", config.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, false));
    clientTimestamps(context.getTimestampGenerator())
        .ifPresent(clientSide -> n.put("client-timestamps", clientSide));
    // 0 legitimately disables the request timeout, and the schema's field is positive-only — but
    // both the field and its enclosing object are optional now, so a disabled timeout is reported
    // by omitting the whole "request" group rather than by emitting a 0 the schema rejects. A
    // positive sub-millisecond timeout, on the other hand, is live (CqlRequestHandler schedules it
    // in nanoseconds) and so must not collapse onto that same 0.
    long requestTimeoutMs =
        positiveMillis(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ZERO));
    if (requestTimeoutMs > 0) {
      ObjectNode request = OBJECT_MAPPER.createObjectNode();
      request.put("timeout-ms", requestTimeoutMs);
      n.set("request", request);
    }
    return n;
  }

  /**
   * Whether the timestamp generator in force assigns the write timestamp client-side, or {@link
   * Optional#empty()} when it is not one this class recognizes.
   *
   * <p>Read by naming the driver's own generators rather than through an accessor on {@link
   * TimestampGenerator}, deliberately: an implementation is free to return {@link
   * Statement#NO_DEFAULT_TIMESTAMP} from {@code next()} and leave the timestamp to the coordinator,
   * which nothing short of calling {@code next()} — and thereby consuming a timestamp — could
   * detect. The interface therefore cannot answer for an arbitrary implementation, and a default
   * answer on it would have named the wrong source for every write a session that never considered
   * the question makes. Unknown is instead said by omission, which is what the schema's optional
   * field is for.
   *
   * <p>The two client-side branches are the complete set: {@code MonotonicTimestampGenerator},
   * which both of them extend, is package-private, so nothing outside its own package can inherit
   * its behavior without going through one of these two.
   *
   * <p>{@code instanceof}, not the exact-class checks the policy branches use: this reads a
   * property the generator has rather than deciding which built-in is in force, and a subclass
   * inherits the {@code next()} that supplies it.
   */
  private static Optional<Boolean> clientTimestamps(TimestampGenerator generator) {
    if (generator instanceof AtomicTimestampGenerator
        || generator instanceof ThreadLocalTimestampGenerator) {
      return Optional.of(true);
    } else if (generator instanceof ServerSideTimestampGenerator) {
      return Optional.of(false);
    }
    return Optional.empty();
  }

  /**
   * TLS settings, or {@code null} when TLS is off — the schema dropped the {@code enabled} boolean,
   * so presence of the group is what reports that it is on.
   */
  @Nullable
  private ObjectNode tls(Channel channel) {
    // TLS is on exactly when the channel pipeline gets an SSL handler, which ChannelFactory decides
    // from the low-level SslHandlerFactory. Deliberately not getSslEngineFactory(): that is only
    // the public JDK-based path that DefaultDriverContext.buildSslHandlerFactory() wraps, and an
    // override of that method (the documented expert extension point, e.g. Netty's native OpenSSL)
    // supplies a handler factory with no engine factory at all — a session that is encrypted all
    // the same.
    Optional<SslHandlerFactory> handlerFactory = context.getSslHandlerFactory();
    if (!handlerFactory.isPresent()) {
      return null;
    }
    ObjectNode n = OBJECT_MAPPER.createObjectNode();
    // Host name validation, on the other hand, is read from the SSLParameters of the engine the
    // JdkSslHandlerFactory actually wrapped for the connection, but only when the built-in factory
    // also knows which trust-manager path interprets those parameters. An arbitrary extended trust
    // manager can ignore a nonempty endpoint-identification algorithm or verify names without one.
    // Anything else (a native-OpenSSL handler, a bespoke or programmatic JDK context) leaves it
    // unknown, and the schema's field is optional precisely so that unknown can be said by
    // omission. Exact-class check, like the policy branches above:
    // JdkSslHandlerFactory is not final, and a subclass need not use the engine it was given.
    //
    // Note this is the engine's own state, not the SSL_HOSTNAME_VALIDATION config option. A factory
    // supplied through SessionBuilder.withSslContext(...) ignores that option, and its arbitrary
    // SSLContext may enforce or ignore host names independently of the endpoint-identification
    // algorithm; that case is therefore unknown rather than guessed from config or parameters.
    SslHandlerFactory factory = handlerFactory.get();
    if (factory.getClass() == JdkSslHandlerFactory.class) {
      Boolean hostnameValidationRequired =
          ((JdkSslHandlerFactory) factory).getHostnameValidationRequired(channel);
      if (hostnameValidationRequired != null) {
        n.put("hostname-verification", hostnameValidationRequired);
      }
    }
    return n;
  }

  /**
   * A duration in milliseconds, floored at 1 for any strictly positive duration, and 0 for a zero
   * or negative one.
   *
   * <p>The schema measures every duration in whole milliseconds, but the driver holds these options
   * as {@link java.time.Duration} and schedules several of them in nanoseconds — so truncating
   * would turn a live sub-millisecond timeout into the 0 that these fields define as "disabled"
   * (or, for {@code schema-agreement.timeout-ms}, "do not wait"). Flooring at 1 keeps the reported
   * value inside the schema's positive range and on the right side of that distinction; it costs at
   * most one millisecond of precision on a setting the schema could not have expressed exactly
   * anyway.
   */
  private static long positiveMillis(Duration duration) {
    return duration.isZero() || duration.isNegative() ? 0L : Math.max(1L, duration.toMillis());
  }

  /**
   * The string as configured, or {@code null} if it is blank.
   *
   * <p>Deliberately not trimmed. {@code OptionalLocalDcHelper} and {@code OptionalLocalRackHelper}
   * hand the configured string to the policy verbatim and match it against a node's datacenter with
   * {@code Objects.equals}, so a padded {@code " dc1 "} is a datacenter that matches no node — and
   * reporting it as {@code dc1} would hide exactly the typo an operator is reading this report to
   * find. The schema's {@code nonEmptyString} only requires a length of at least 1, so the padded
   * form is valid to emit as is.
   *
   * <p>Blank is the one case that cannot be passed through: {@code nonEmptyString} leaves no way to
   * report {@code ""}, and a {@code type: "dc"} preference with the key omitted is invalid too. So
   * a blank value is reported as no preference at all, which diverges from the helpers — they treat
   * it as a set-but-unmatchable datacenter — but is the only schema-valid reading available.
   */
  @Nullable
  private static String blankToNull(@Nullable String s) {
    return s == null || s.trim().isEmpty() ? null : s;
  }

  private void customPolicy(ObjectNode node, Object policy) {
    node.put("type", "custom");
    node.put("name", policyName(policy.getClass()));
  }

  /**
   * A name for a user-supplied policy class, never empty — the schema requires a non-empty string.
   *
   * <p>{@link #simpleName} is empty for an anonymous class (a common way to supply a one-off
   * policy), and throws {@link InternalError} for certain synthetic class names. Both fall back to
   * the full (binary) name, which is always available and still identifies the policy.
   *
   * <p>That {@code InternalError} is the one {@code Error} the reporter knows how to provoke, which
   * is why the catch lives here rather than around the whole report build (see {@link
   * #populateControlConnectionOptions}); it is deliberately {@code InternalError} rather than
   * {@code Error}, so an {@code OutOfMemoryError} or {@code StackOverflowError} still propagates.
   */
  private String policyName(Class<?> policyClass) {
    String name;
    try {
      name = simpleName(policyClass);
    } catch (InternalError e) {
      LOG.debug("Could not read a policy class's simple name; falling back to its binary name", e);
      name = "";
    }
    return name.isEmpty() ? policyClass.getName() : name;
  }

  /**
   * {@link Class#getSimpleName()}, which has a documented JDK edge case throwing {@link
   * InternalError} for certain synthetic class names — reachable here because the class is
   * arbitrary and user-supplied.
   *
   * <p>Package-private, and its own method, purely so that the test in this package can override it
   * to raise that error: no class an ordinary test can declare provokes it. Same seam as {@link
   * #buildJson}.
   */
  String simpleName(Class<?> policyClass) {
    return policyClass.getSimpleName();
  }
}
