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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * Utility component to work with the tokens of a given driver instance.
 *
 * <p>Note that the methods that take a keyspace argument are based on schema metadata, which can be
 * disabled or restricted to a subset of keyspaces; therefore these methods might return empty
 * results for some or all of the keyspaces.
 *
 * @see DefaultDriverOption#METADATA_SCHEMA_ENABLED
 * @see Session#setSchemaMetadataEnabled(Boolean)
 * @see DefaultDriverOption#METADATA_SCHEMA_REFRESHED_KEYSPACES
 */
public interface TokenMap {

  /** Builds a token from its string representation. */
  @NonNull
  Token parse(@NonNull String tokenString);

  /** Formats a token into a string representation appropriate for concatenation in a CQL query. */
  @NonNull
  String format(@NonNull Token token);

  /**
   * Builds a token from a partition key.
   *
   * @param partitioner the partitioner to use or {@code null} for this TokenMap's partitioner.
   * @param partitionKey the partition key components, in their serialized form (which can be
   *     obtained with {@link TypeCodec#encode(Object, ProtocolVersion)}. Neither the individual
   *     components, nor the vararg array itself, can be {@code null}.
   */
  @NonNull
  Token newToken(@Nullable Partitioner partitioner, @NonNull ByteBuffer... partitionKey);

  /** Shortcut for {@link #newToken(Partitioner, ByteBuffer...) newToken(null, partitionKey)}. */
  @NonNull
  default Token newToken(@NonNull ByteBuffer... partitionKey) {
    return newToken(null, partitionKey);
  }

  @NonNull
  TokenRange newTokenRange(@NonNull Token start, @NonNull Token end);

  /** The token ranges that define data distribution on the ring. */
  @NonNull
  Set<TokenRange> getTokenRanges();

  /** The token ranges for which a given node is the primary replica. */
  @NonNull
  Set<TokenRange> getTokenRanges(Node node);

  /**
   * The tokens owned by the given node.
   *
   * <p>This is functionally equivalent to {@code getTokenRanges(node).map(r -> r.getEnd())}. Note
   * that the set is rebuilt every time you call this method.
   */
  @NonNull
  default Set<Token> getTokens(@NonNull Node node) {
    ImmutableSet.Builder<Token> result = ImmutableSet.builder();
    for (TokenRange range : getTokenRanges(node)) {
      result.add(range.getEnd());
    }
    return result.build();
  }

  /** The token ranges that are replicated on the given node, for the given keyspace. */
  @NonNull
  Set<TokenRange> getTokenRanges(@NonNull CqlIdentifier keyspace, @NonNull Node replica);

  /**
   * Shortcut for {@link #getTokenRanges(CqlIdentifier, Node)
   * getTokenRanges(CqlIdentifier.fromCql(keyspaceName), replica)}.
   */
  @NonNull
  default Set<TokenRange> getTokenRanges(@NonNull String keyspaceName, @NonNull Node replica) {
    return getTokenRanges(CqlIdentifier.fromCql(keyspaceName), replica);
  }

  /**
   * The replicas for a given partition key in the given keyspace.
   *
   * @deprecated Use {@link #getReplicasList(CqlIdentifier, Partitioner, ByteBuffer)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(
      @NonNull CqlIdentifier keyspace,
      @Nullable Partitioner partitioner,
      @NonNull ByteBuffer partitionKey) {
    return ImmutableSet.copyOf(getReplicasList(keyspace, partitioner, partitionKey));
  }

  /**
   * The replicas for a given partition key in the given keyspace, in ring order.
   *
   * @param keyspace the keyspace for which to get replicas
   * @param partitioner the partitioner to use or {@code null} for this TokenMap's partitioner
   * @param partitionKey the partition key in its serialized form
   * @return ordered list of replica nodes for the given partition key
   */
  @NonNull
  List<Node> getReplicasList(
      @NonNull CqlIdentifier keyspace,
      @Nullable Partitioner partitioner,
      @NonNull ByteBuffer partitionKey);

  /**
   * Shortcut for {@link #getReplicas(CqlIdentifier, Partitioner, ByteBuffer) getReplicas(keyspace,
   * null, partitionKey)}.
   *
   * @deprecated Use {@link #getReplicasList(CqlIdentifier, ByteBuffer)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(@NonNull CqlIdentifier keyspace, @NonNull ByteBuffer partitionKey) {
    return getReplicas(keyspace, null, partitionKey);
  }

  /**
   * Shortcut for {@link #getReplicasList(CqlIdentifier, Partitioner, ByteBuffer)
   * getReplicasList(keyspace, null, partitionKey)}.
   *
   * @param keyspace the keyspace for which to get replicas
   * @param partitionKey the partition key in its serialized form
   * @return ordered list of replica nodes for the given partition key
   */
  @NonNull
  default List<Node> getReplicasList(
      @NonNull CqlIdentifier keyspace, @NonNull ByteBuffer partitionKey) {
    return getReplicasList(keyspace, null, partitionKey);
  }

  /**
   * Shortcut for {@link #getReplicas(CqlIdentifier, Partitioner, ByteBuffer)
   * getReplicas(CqlIdentifier.fromCql(keyspaceName), partitioner, partitionKey)}.
   *
   * @deprecated Use {@link #getReplicasList(String, Partitioner, ByteBuffer)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(
      @NonNull String keyspaceName,
      @Nullable Partitioner partitioner,
      @NonNull ByteBuffer partitionKey) {
    return getReplicas(CqlIdentifier.fromCql(keyspaceName), partitioner, partitionKey);
  }

  /**
   * Shortcut for {@link #getReplicasList(CqlIdentifier, Partitioner, ByteBuffer)
   * getReplicasList(CqlIdentifier.fromCql(keyspaceName), partitioner, partitionKey)}.
   *
   * @param keyspaceName the keyspace name for which to get replicas
   * @param partitioner the partitioner to use or {@code null} for this TokenMap's partitioner
   * @param partitionKey the partition key in its serialized form
   * @return ordered list of replica nodes for the given partition key
   */
  @NonNull
  default List<Node> getReplicasList(
      @NonNull String keyspaceName,
      @Nullable Partitioner partitioner,
      @NonNull ByteBuffer partitionKey) {
    return getReplicasList(CqlIdentifier.fromCql(keyspaceName), partitioner, partitionKey);
  }

  /**
   * Shortcut for {@link #getReplicas(CqlIdentifier, Partitioner, ByteBuffer)
   * getReplicas(CqlIdentifier.fromCql(keyspaceName), null, partitionKey)}.
   *
   * @deprecated Use {@link #getReplicasList(String, ByteBuffer)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(@NonNull String keyspaceName, @NonNull ByteBuffer partitionKey) {
    return getReplicas(CqlIdentifier.fromCql(keyspaceName), null, partitionKey);
  }

  /**
   * Shortcut for {@link #getReplicasList(CqlIdentifier, Partitioner, ByteBuffer)
   * getReplicasList(CqlIdentifier.fromCql(keyspaceName), null, partitionKey)}.
   *
   * @param keyspaceName the keyspace name for which to get replicas
   * @param partitionKey the partition key in its serialized form
   * @return ordered list of replica nodes for the given partition key
   */
  @NonNull
  default List<Node> getReplicasList(
      @NonNull String keyspaceName, @NonNull ByteBuffer partitionKey) {
    return getReplicasList(CqlIdentifier.fromCql(keyspaceName), null, partitionKey);
  }

  /**
   * The replicas for a given token in the given keyspace.
   *
   * @deprecated Use {@link #getReplicasList(CqlIdentifier, Token)} instead.
   */
  @NonNull
  @Deprecated
  Set<Node> getReplicas(@NonNull CqlIdentifier keyspace, @NonNull Token token);

  /**
   * The replicas for a given token in the given keyspace, in ring order.
   *
   * @param keyspace the keyspace for which to get replicas
   * @param token the token for which to get replicas
   * @return ordered list of replica nodes for the given token
   */
  @NonNull
  default List<Node> getReplicasList(@NonNull CqlIdentifier keyspace, @NonNull Token token) {
    return ImmutableList.copyOf(getReplicas(keyspace, token));
  }

  /**
   * Shortcut for {@link #getReplicas(CqlIdentifier, Token)
   * getReplicas(CqlIdentifier.fromCql(keyspaceName), token)}.
   *
   * @deprecated Use {@link #getReplicasList(String, Token)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(@NonNull String keyspaceName, @NonNull Token token) {
    return getReplicas(CqlIdentifier.fromCql(keyspaceName), token);
  }

  /**
   * Shortcut for {@link #getReplicasList(CqlIdentifier, Token)
   * getReplicasList(CqlIdentifier.fromCql(keyspaceName), token)}.
   *
   * @param keyspaceName the keyspace name for which to get replicas
   * @param token the token for which to get replicas
   * @return ordered list of replica nodes for the given token
   */
  @NonNull
  default List<Node> getReplicasList(@NonNull String keyspaceName, @NonNull Token token) {
    return getReplicasList(CqlIdentifier.fromCql(keyspaceName), token);
  }

  /**
   * The replicas for a given range in the given keyspace.
   *
   * <p>It is assumed that the input range does not overlap across multiple node ranges. If the
   * range extends over multiple nodes, it only returns the nodes that are replicas for the last
   * token of the range. In other words, this method is a shortcut for {@code getReplicas(keyspace,
   * range.getEnd())}.
   *
   * @deprecated Use {@link #getReplicasList(CqlIdentifier, TokenRange)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(@NonNull CqlIdentifier keyspace, @NonNull TokenRange range) {
    return getReplicas(keyspace, range.getEnd());
  }

  /**
   * The replicas for a given range in the given keyspace, in ring order.
   *
   * <p>It is assumed that the input range does not overlap across multiple node ranges. If the
   * range extends over multiple nodes, it only returns the nodes that are replicas for the last
   * token of the range. In other words, this method is a shortcut for {@code
   * getReplicasList(keyspace, range.getEnd())}.
   *
   * @param keyspace the keyspace for which to get replicas
   * @param range the token range for which to get replicas
   * @return ordered list of replica nodes for the given token range
   */
  @NonNull
  default List<Node> getReplicasList(@NonNull CqlIdentifier keyspace, @NonNull TokenRange range) {
    return getReplicasList(keyspace, range.getEnd());
  }

  /**
   * Shortcut for {@link #getReplicas(CqlIdentifier, TokenRange)
   * getReplicas(CqlIdentifier.fromCql(keyspaceName), range)}.
   *
   * @deprecated Use {@link #getReplicasList(String, TokenRange)} instead.
   */
  @NonNull
  @Deprecated
  default Set<Node> getReplicas(@NonNull String keyspaceName, @NonNull TokenRange range) {
    return getReplicas(CqlIdentifier.fromCql(keyspaceName), range);
  }

  /**
   * Shortcut for {@link #getReplicasList(CqlIdentifier, TokenRange)
   * getReplicasList(CqlIdentifier.fromCql(keyspaceName), range)}.
   *
   * @param keyspaceName the keyspace name for which to get replicas
   * @param range the token range for which to get replicas
   * @return ordered list of replica nodes for the given token range
   */
  @NonNull
  default List<Node> getReplicasList(@NonNull String keyspaceName, @NonNull TokenRange range) {
    return getReplicasList(CqlIdentifier.fromCql(keyspaceName), range);
  }

  /** The name of the partitioner class in use, as reported by the Cassandra nodes. */
  @NonNull
  String getPartitionerName();

  /** The partitioner class in use, as reported by the Cassandra nodes. */
  @NonNull
  Partitioner getPartitioner();
}
