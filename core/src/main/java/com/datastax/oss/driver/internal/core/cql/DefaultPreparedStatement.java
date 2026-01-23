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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CQL4SkipMetadataResolveMethod;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.ContainerType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DefaultPreparedStatement implements PreparedStatement {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPreparedStatement.class);
  private static final Splitter SPACE_SPLITTER = Splitter.onPattern("\\s+");
  private static final Splitter COMMA_SPLITTER = Splitter.onPattern(",");

  private final ByteBuffer id;
  private final RepreparePayload repreparePayload;
  private final ColumnDefinitions variableDefinitions;
  private final List<Integer> partitionKeyIndices;
  private volatile ResultMetadata resultMetadata;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;
  private final String executionProfileNameForBoundStatements;
  private final DriverExecutionProfile executionProfileForBoundStatements;
  private final ByteBuffer pagingStateForBoundStatements;
  private final CqlIdentifier routingKeyspaceForBoundStatements;
  private final ByteBuffer routingKeyForBoundStatements;
  private final Token routingTokenForBoundStatements;
  private final Map<String, ByteBuffer> customPayloadForBoundStatements;
  private final Boolean areBoundStatementsIdempotent;
  private final boolean areBoundStatementsTracing;
  private final int pageSizeForBoundStatements;
  private final ConsistencyLevel consistencyLevelForBoundStatements;
  private final ConsistencyLevel serialConsistencyLevelForBoundStatements;
  private final Duration timeoutForBoundStatements;
  private final Partitioner partitioner;
  @NonNull private final RequestRoutingType requestRoutingType;
  private volatile boolean skipMetadata;

  public DefaultPreparedStatement(
      ByteBuffer id,
      String query,
      ColumnDefinitions variableDefinitions,
      List<Integer> partitionKeyIndices,
      ByteBuffer resultMetadataId,
      ColumnDefinitions resultSetDefinitions,
      CqlIdentifier keyspace,
      Partitioner partitioner,
      Map<String, ByteBuffer> customPayloadForPrepare,
      String executionProfileNameForBoundStatements,
      DriverExecutionProfile executionProfileForBoundStatements,
      CqlIdentifier routingKeyspaceForBoundStatements,
      ByteBuffer routingKeyForBoundStatements,
      Token routingTokenForBoundStatements,
      Map<String, ByteBuffer> customPayloadForBoundStatements,
      Boolean areBoundStatementsIdempotent,
      Duration timeoutForBoundStatements,
      ByteBuffer pagingStateForBoundStatements,
      int pageSizeForBoundStatements,
      ConsistencyLevel consistencyLevelForBoundStatements,
      ConsistencyLevel serialConsistencyLevelForBoundStatements,
      boolean areBoundStatementsTracing,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion,
      @NonNull RequestRoutingType requestRoutingType) {
    this.id = id;
    this.partitionKeyIndices = partitionKeyIndices;
    // It's important that we keep a reference to this object, so that it only gets evicted from
    // the map in DefaultSession if no client reference the PreparedStatement anymore.
    this.repreparePayload = new RepreparePayload(id, query, keyspace, customPayloadForPrepare);
    this.variableDefinitions = variableDefinitions;
    this.resultMetadata = new ResultMetadata(resultMetadataId, resultSetDefinitions);

    this.executionProfileNameForBoundStatements = executionProfileNameForBoundStatements;
    this.executionProfileForBoundStatements = executionProfileForBoundStatements;
    this.routingKeyspaceForBoundStatements = routingKeyspaceForBoundStatements;
    this.routingKeyForBoundStatements = routingKeyForBoundStatements;
    this.routingTokenForBoundStatements = routingTokenForBoundStatements;
    this.customPayloadForBoundStatements = customPayloadForBoundStatements;
    this.areBoundStatementsIdempotent = areBoundStatementsIdempotent;
    this.timeoutForBoundStatements = timeoutForBoundStatements;
    this.pagingStateForBoundStatements = pagingStateForBoundStatements;
    this.pageSizeForBoundStatements = pageSizeForBoundStatements;
    this.consistencyLevelForBoundStatements = consistencyLevelForBoundStatements;
    this.serialConsistencyLevelForBoundStatements = serialConsistencyLevelForBoundStatements;
    this.areBoundStatementsTracing = areBoundStatementsTracing;
    this.partitioner = partitioner;

    this.codecRegistry = codecRegistry;
    this.protocolVersion = protocolVersion;
    this.requestRoutingType = requestRoutingType;
    this.skipMetadata =
        resolveSkipMetadata(
            query, resultMetadataId, resultSetDefinitions, this.executionProfileForBoundStatements);
  }

  @NonNull
  @Override
  public ByteBuffer getId() {
    return id;
  }

  @NonNull
  @Override
  public String getQuery() {
    return repreparePayload.query;
  }

  @NonNull
  @Override
  public ColumnDefinitions getVariableDefinitions() {
    return variableDefinitions;
  }

  @Override
  public Partitioner getPartitioner() {
    return partitioner;
  }

  public boolean isSkipMetadata() {
    return skipMetadata;
  }

  @NonNull
  @Override
  public List<Integer> getPartitionKeyIndices() {
    return partitionKeyIndices;
  }

  @Override
  public ByteBuffer getResultMetadataId() {
    return resultMetadata.resultMetadataId;
  }

  @NonNull
  @Override
  public ColumnDefinitions getResultSetDefinitions() {
    return resultMetadata.resultSetDefinitions;
  }

  @Override
  public boolean isLWT() {
    return requestRoutingType == RequestRoutingType.LWT;
  }

  @Override
  public void setResultMetadata(
      @NonNull ByteBuffer newResultMetadataId, @NonNull ColumnDefinitions newResultSetDefinitions) {
    this.skipMetadata =
        resolveSkipMetadata(
            this.getQuery(),
            newResultMetadataId,
            newResultSetDefinitions,
            executionProfileForBoundStatements);

    this.resultMetadata = new ResultMetadata(newResultMetadataId, newResultSetDefinitions);
  }

  @NonNull
  @Override
  public BoundStatement bind(@NonNull Object... values) {
    return new DefaultBoundStatement(
        this,
        variableDefinitions,
        ValuesHelper.encodePreparedValues(
            values, variableDefinitions, codecRegistry, protocolVersion),
        executionProfileNameForBoundStatements,
        executionProfileForBoundStatements,
        routingKeyspaceForBoundStatements,
        routingKeyForBoundStatements,
        routingTokenForBoundStatements,
        customPayloadForBoundStatements,
        areBoundStatementsIdempotent,
        areBoundStatementsTracing,
        Statement.NO_DEFAULT_TIMESTAMP,
        pagingStateForBoundStatements,
        pageSizeForBoundStatements,
        consistencyLevelForBoundStatements,
        serialConsistencyLevelForBoundStatements,
        timeoutForBoundStatements,
        codecRegistry,
        protocolVersion,
        null,
        Statement.NO_NOW_IN_SECONDS,
        requestRoutingType);
  }

  @NonNull
  @Override
  public BoundStatementBuilder boundStatementBuilder(@NonNull Object... values) {
    return new BoundStatementBuilder(
        this,
        variableDefinitions,
        ValuesHelper.encodePreparedValues(
            values, variableDefinitions, codecRegistry, protocolVersion),
        executionProfileNameForBoundStatements,
        executionProfileForBoundStatements,
        routingKeyspaceForBoundStatements,
        routingKeyForBoundStatements,
        routingTokenForBoundStatements,
        customPayloadForBoundStatements,
        areBoundStatementsIdempotent,
        areBoundStatementsTracing,
        Statement.NO_DEFAULT_TIMESTAMP,
        pagingStateForBoundStatements,
        pageSizeForBoundStatements,
        consistencyLevelForBoundStatements,
        serialConsistencyLevelForBoundStatements,
        timeoutForBoundStatements,
        codecRegistry,
        protocolVersion);
  }

  public RepreparePayload getRepreparePayload() {
    return this.repreparePayload;
  }

  private static class ResultMetadata {
    private final ByteBuffer resultMetadataId;
    private final ColumnDefinitions resultSetDefinitions;

    private ResultMetadata(ByteBuffer resultMetadataId, ColumnDefinitions resultSetDefinitions) {
      this.resultMetadataId = resultMetadataId;
      this.resultSetDefinitions = resultSetDefinitions;
    }
  }

  private static boolean resolveSkipMetadata(
      String query,
      ByteBuffer resultMetadataId,
      ColumnDefinitions resultSet,
      DriverExecutionProfile executionProfileForBoundStatements) {
    if (resultSet == null || resultSet.size() == 0) {
      // there is no reason to send this flag, there will be no rows in the response and,
      // consequently, no metadata.
      return false;
    }
    if (resultMetadataId != null && resultMetadataId.capacity() > 0) {
      // Result metadata ID feature is supported, it makes prepared statement invalidation work
      // properly.
      // Skip Metadata should be enabled.
      // Prepared statement invalidation works perfectly no need to disable skip metadata
      return true;
    }

    CQL4SkipMetadataResolveMethod resolveMethod = CQL4SkipMetadataResolveMethod.SMART;

    if (executionProfileForBoundStatements != null) {
      String resolveMethodName =
          executionProfileForBoundStatements.getString(
              DefaultDriverOption.PREPARE_SKIP_CQL4_METADATA_RESOLVE_METHOD);
      try {
        resolveMethod = CQL4SkipMetadataResolveMethod.fromValue(resolveMethodName);
      } catch (IllegalArgumentException e) {
        LOGGER.warn(
            "Property advanced.prepared-statements.skip-cql4-metadata-resolve-method is incorrectly set to `{}`, "
                + "available options: smart, enabled, disabled. Defaulting to `SMART`",
            resolveMethodName);
        resolveMethod = CQL4SkipMetadataResolveMethod.SMART;
      }
    }

    switch (resolveMethod) {
      case ENABLED:
        return true;
      case DISABLED:
        return false;
      case SMART:
        break;
    }

    if (isWildcardSelect(query)) {
      LOGGER.warn(
          "Prepared statement {} is a wildcard select, which can cause prepared statement invalidation issues when executed on CQL4. "
              + "These issues may lead to broken deserialization or data corruption. "
              + "To mitigate this, the driver ensures that the server returns metadata with each query for such statements, "
              + "though this negatively impacts performance. "
              + "To avoid this, consider using a targeted select instead. "
              + "Find more mitigation options in description of `advanced.prepared-statements.skip-cql4-metadata-resolve-method` flag",
          query);
      return false;
    }
    // Disable skipping metadata if results contains udt and
    for (ColumnDefinition columnDefinition : resultSet) {
      if (containsUDT(columnDefinition.getType())) {
        LOGGER.warn(
            "Prepared statement {} contains UDT in result, which can cause prepared statement invalidation issues when executed on CQL4. "
                + "These issues may lead to broken deserialization or data corruption. "
                + "To mitigate this, the driver ensures that the server returns metadata with each query for such statements, "
                + "though this negatively impacts performance. "
                + "To avoid this, consider using regular columns instead of UDT. "
                + "Find more mitigation options in description of `advanced.prepared-statements.skip-cql4-metadata-resolve-method` flag",
            query);
        return false;
      }
    }
    return true;
  }

  private static boolean containsUDT(DataType dataType) {
    if (dataType instanceof ContainerType) {
      return containsUDT(((ContainerType) dataType).getElementType());
    } else if (dataType instanceof TupleType) {
      for (DataType elementType : ((TupleType) dataType).getComponentTypes()) {
        if (containsUDT(elementType)) {
          return true;
        }
      }
      return false;
    } else if (dataType instanceof MapType) {
      return containsUDT(((MapType) dataType).getKeyType())
          || containsUDT(((MapType) dataType).getValueType());
    }
    return dataType instanceof UserDefinedType;
  }

  private static boolean isWildcardSelect(String query) {
    List<String> chunks = SPACE_SPLITTER.splitToList(query.trim().toLowerCase());
    if (chunks.size() < 2) {
      // Weird query, assuming no result expected
      return false;
    }

    if (!chunks.get(0).equals("select")) {
      // In case if non-select sneaks in, disable skip metadata for it no result expected.
      return false;
    }

    for (String chunk : chunks) {
      if (chunk.equals("from")) {
        return false;
      }
      if (chunk.equals("*")) {
        return true;
      }
      for (String part : COMMA_SPLITTER.split(chunk)) {
        if (part.equals("*")) {
          return true;
        }
      }
    }
    return false;
  }
}
