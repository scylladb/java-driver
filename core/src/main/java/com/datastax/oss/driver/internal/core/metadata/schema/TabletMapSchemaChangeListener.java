package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import edu.umd.cs.findbugs.annotations.NonNull;

public class TabletMapSchemaChangeListener extends SchemaChangeListenerBase {
  private final MetadataManager manager;

  public TabletMapSchemaChangeListener(MetadataManager manager) {
    this.manager = manager;
  }

  @Override
  public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
    if (!manager.getMetadata().getTabletMap().isPresent()) {
      return;
    }
    manager.getMetadata().getTabletMap().get().removeByKeyspace(keyspace.getName());
  }

  @Override
  public void onKeyspaceUpdated(
      @NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
    if (!manager.getMetadata().getTabletMap().isPresent()) {
      return;
    }
    manager.getMetadata().getTabletMap().get().removeByKeyspace(previous.getName());
  }

  @Override
  public void onTableDropped(@NonNull TableMetadata table) {
    if (!manager.getMetadata().getTabletMap().isPresent()) {
      return;
    }
    manager.getMetadata().getTabletMap().get().removeByTable(table.getName());
  }

  @Override
  public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
    if (!manager.getMetadata().getTabletMap().isPresent()) {
      return;
    }
    manager.getMetadata().getTabletMap().get().removeByTable(previous.getName());
  }
}
