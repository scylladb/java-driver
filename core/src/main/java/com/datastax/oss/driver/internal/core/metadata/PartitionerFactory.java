package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.CDCTokenFactory;
import java.util.Map;
import java.util.Optional;

/**
 * Returns correct Partitioner to use for given query, for example @code{CDCTokenFactory} for
 * queries to CDC log table.
 */
public class PartitionerFactory {
  private static final String SCYLLA_CDC_LOG_SUFFIX = "_scylla_cdc_log";
  private static final String SCYLLA_CDC_EXTENSION = "cdc";

  public static Partitioner partitioner(
      ColumnDefinitions variableDefinitions, InternalDriverContext context) {
    if (variableDefinitions == null || variableDefinitions.size() == 0) {
      return null;
    }

    String keyspace = variableDefinitions.get(0).getKeyspace().toString();
    String table = variableDefinitions.get(0).getTable().toString();
    if (table.endsWith(SCYLLA_CDC_LOG_SUFFIX)) {
      String baseTableName = table.substring(0, table.length() - SCYLLA_CDC_LOG_SUFFIX.length());
      Optional<KeyspaceMetadata> keyspaceMetadata =
          context.getMetadataManager().getMetadata().getKeyspace(keyspace);
      if (!keyspaceMetadata.isPresent()) {
        return null;
      }
      Optional<TableMetadata> tableMetadata = keyspaceMetadata.get().getTable(baseTableName);
      Optional<Object> extensions =
          tableMetadata.map(q -> q.getOptions().get(CqlIdentifier.fromCql("extensions")));
      if (extensions.isPresent() && extensions.get() instanceof Map) {
        Map<String, Object> extensionsMap = (Map<String, Object>) extensions.get();
        if (extensionsMap.containsKey(SCYLLA_CDC_EXTENSION)) {
          return new CDCTokenFactory();
        }
      }
    }

    return null;
  }
}
