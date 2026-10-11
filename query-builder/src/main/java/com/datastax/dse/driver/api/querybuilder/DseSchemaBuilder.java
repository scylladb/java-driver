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
package com.datastax.dse.driver.api.querybuilder;

import com.datastax.dse.driver.api.querybuilder.schema.AlterDseKeyspaceStart;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseAggregateStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseKeyspaceStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseTableStart;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateAggregateStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionStart;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An extension of {@link com.datastax.oss.driver.api.querybuilder.SchemaBuilder} for building
 * schema entities that have DSE specific functionality.
 *
 * @deprecated DSE-specific query building is no longer supported. This type is retained for binary
 *     compatibility.
 */
@Deprecated
@SuppressWarnings("DoNotCallSuggester")
public class DseSchemaBuilder extends SchemaBuilder {

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(@NonNull CqlIdentifier aggregateId) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(
      @Nullable CqlIdentifier keyspaceId, @NonNull CqlIdentifier aggregateId) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createDseAggregate(CqlIdentifier)
   * createDseAggregate(CqlIdentifier.fromCql(aggregateName))}.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(@NonNull String aggregateName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)
   * createDseAggregate(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(aggregateName))}.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(
      @Nullable String keyspaceName, @NonNull String aggregateName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(CqlIdentifier)}.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(@NonNull CqlIdentifier aggregateName) {
    return SchemaBuilder.createAggregate(aggregateName);
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(CqlIdentifier, CqlIdentifier)}.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier aggregateName) {
    return SchemaBuilder.createAggregate(keyspace, aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier)
   * createAggregate(CqlIdentifier.fromCql(aggregateName)}.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(String)}.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(@NonNull String aggregateName) {
    return SchemaBuilder.createAggregate(aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)
   * createAggregate(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(aggregateName)}.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(String, String)}.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(
      @Nullable String keyspace, @NonNull String aggregateName) {
    return SchemaBuilder.createAggregate(keyspace, aggregateName);
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(@NonNull CqlIdentifier functionId) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name for the given keyspace name.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(
      @Nullable CqlIdentifier keyspaceId, @NonNull CqlIdentifier functionId) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(functionName)}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(@NonNull String functionName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName)}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(
      @Nullable String keyspaceName, @NonNull String functionName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(CqlIdentifier)}.
   */
  @NonNull
  public static CreateFunctionStart createFunction(@NonNull CqlIdentifier functionName) {
    return SchemaBuilder.createFunction(functionName);
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name for the given keyspace name.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(CqlIdentifier,CqlIdentifier)}.
   */
  @NonNull
  public static CreateFunctionStart createFunction(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    return SchemaBuilder.createFunction(keyspace, functionName);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspace, functionName)}
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(String)}.
   */
  @NonNull
  public static CreateFunctionStart createFunction(@NonNull String functionName) {
    return SchemaBuilder.createFunction(functionName);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(functionName)}.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(String, String)}.
   */
  @NonNull
  public static CreateFunctionStart createFunction(
      @Nullable String keyspace, @NonNull String functionName) {
    return SchemaBuilder.createFunction(keyspace, functionName);
  }

  /**
   * Starts a CREATE KEYSPACE query.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseKeyspaceStart createDseKeyspace(@NonNull CqlIdentifier keyspaceName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createDseKeyspace(CqlIdentifier)
   * createKeyspace(CqlIdentifier.fromCql(keyspaceName))}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseKeyspaceStart createDseKeyspace(@NonNull String keyspaceName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts an ALTER KEYSPACE query.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static AlterDseKeyspaceStart alterDseKeyspace(@NonNull CqlIdentifier keyspaceName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #alterDseKeyspace(CqlIdentifier)
   * alterKeyspace(CqlIdentifier.fromCql(keyspaceName)}.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static AlterDseKeyspaceStart alterDseKeyspace(@NonNull String keyspaceName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts a CREATE TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseTableStart createDseTable(@NonNull CqlIdentifier tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts a CREATE TABLE query with the given table name for the given keyspace name.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseTableStart createDseTable(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createDseTable(CqlIdentifier)
   * createDseTable(CqlIdentifier.fromCql(tableName)}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseTableStart createDseTable(@NonNull String tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #createDseTable(CqlIdentifier,CqlIdentifier)
   * createDseTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static CreateDseTableStart createDseTable(
      @Nullable String keyspace, @NonNull String tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts an ALTER TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(@NonNull CqlIdentifier tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Starts an ALTER TABLE query with the given table name for the given keyspace name.
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #alterDseTable(CqlIdentifier)
   * alterDseTable(CqlIdentifier.fromCql(tableName)}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(@NonNull String tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }

  /**
   * Shortcut for {@link #alterDseTable(CqlIdentifier,CqlIdentifier)
   * alterDseTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   *
   * @throws UnsupportedOperationException always; DSE schema query building is unsupported.
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(
      @Nullable String keyspace, @NonNull String tableName) {
    throw new UnsupportedOperationException("DSE schema query building is no longer supported");
  }
}
