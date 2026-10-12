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
 * A compatibility shell for the former DSE schema builder. Ordinary Cassandra schema methods
 * delegate to {@link SchemaBuilder}.
 *
 * @deprecated DSE-specific schema builders are no longer supported. Their entry points throw {@link
 *     UnsupportedOperationException}. Use {@link SchemaBuilder} for ordinary Cassandra schemas.
 */
@Deprecated
@SuppressWarnings("DoNotCallSuggester")
public class DseSchemaBuilder extends SchemaBuilder {

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(@NonNull CqlIdentifier aggregateId) {
    throw unsupported();
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(
      @Nullable CqlIdentifier keyspaceId, @NonNull CqlIdentifier aggregateId) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #createDseAggregate(CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(@NonNull String aggregateName) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #createDseAggregate(CqlIdentifier, CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseAggregateStart createDseAggregate(
      @Nullable String keyspaceName, @NonNull String aggregateName) {
    throw unsupported();
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(@NonNull CqlIdentifier aggregateName) {
    return SchemaBuilder.createAggregate(aggregateName);
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier aggregateName) {
    return SchemaBuilder.createAggregate(keyspace, aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier)} using CQL-form names.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateAggregateStart createAggregate(@NonNull String aggregateName) {
    return SchemaBuilder.createAggregate(aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)} using CQL-form names.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
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
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(@NonNull CqlIdentifier functionId) {
    throw unsupported();
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name for the given keyspace name.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(
      @Nullable CqlIdentifier keyspaceId, @NonNull CqlIdentifier functionId) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #createDseFunction(CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(@NonNull String functionName) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #createDseFunction(CqlIdentifier, CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseFunctionStart createDseFunction(
      @Nullable String keyspaceName, @NonNull String functionName) {
    throw unsupported();
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateFunctionStart createFunction(@NonNull CqlIdentifier functionName) {
    return SchemaBuilder.createFunction(functionName);
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name for the given keyspace name.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateFunctionStart createFunction(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    return SchemaBuilder.createFunction(keyspace, functionName);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)} using CQL-form names.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateFunctionStart createFunction(@NonNull String functionName) {
    return SchemaBuilder.createFunction(functionName);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)} using CQL-form names.
   *
   * <p>This method only covers open-source Cassandra syntax. DSE-specific features are unsupported.
   */
  @NonNull
  public static CreateFunctionStart createFunction(
      @Nullable String keyspace, @NonNull String functionName) {
    return SchemaBuilder.createFunction(keyspace, functionName);
  }

  /**
   * Starts a CREATE KEYSPACE query.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseKeyspaceStart createDseKeyspace(@NonNull CqlIdentifier keyspaceName) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #createDseKeyspace(CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseKeyspaceStart createDseKeyspace(@NonNull String keyspaceName) {
    return createDseKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * Starts an ALTER KEYSPACE query.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static AlterDseKeyspaceStart alterDseKeyspace(@NonNull CqlIdentifier keyspaceName) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #alterDseKeyspace(CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static AlterDseKeyspaceStart alterDseKeyspace(@NonNull String keyspaceName) {
    return DseSchemaBuilder.alterDseKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * Starts a CREATE TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseTableStart createDseTable(@NonNull CqlIdentifier tableName) {
    return createDseTable(null, tableName);
  }

  /**
   * Starts a CREATE TABLE query with the given table name for the given keyspace name.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseTableStart createDseTable(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #createDseTable(CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseTableStart createDseTable(@NonNull String tableName) {
    return createDseTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #createDseTable(CqlIdentifier, CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static CreateDseTableStart createDseTable(
      @Nullable String keyspace, @NonNull String tableName) {
    return createDseTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts an ALTER TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(@NonNull CqlIdentifier tableName) {
    throw unsupported();
  }

  /**
   * Starts an ALTER TABLE query with the given table name for the given keyspace name.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
    throw unsupported();
  }

  /**
   * Shortcut for {@link #alterDseTable(CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(@NonNull String tableName) {
    return alterDseTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #alterDseTable(CqlIdentifier, CqlIdentifier)} using CQL-form names.
   *
   * @throws UnsupportedOperationException if called with valid arguments; DSE schema construction
   *     is unsupported
   */
  @NonNull
  public static AlterDseTableStart alterDseTable(
      @Nullable String keyspace, @NonNull String tableName) {
    return alterDseTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("DSE schema query building is no longer supported");
  }
}
