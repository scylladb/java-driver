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
package com.datastax.dse.driver.api.core.metadata.schema;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

/**
 * Specialized table metadata, that handles the graph-specific properties introduced in DSE 6.8.
 *
 * @deprecated DSE-specific metadata is no longer populated by the driver. This type is retained for
 *     binary compatibility.
 */
@Deprecated
public interface DseGraphTableMetadata extends DseTableMetadata {
  /**
   * The vertex metadata if this table represents a vertex in graph, otherwise empty.
   *
   * <p>This is mutually exclusive with {@link #getEdge()}.
   */
  @NonNull
  Optional<DseVertexMetadata> getVertex();

  /**
   * The edge metadata if this table represents an edge in graph, otherwise empty.
   *
   * <p>This is mutually exclusive with {@link #getVertex()}.
   */
  @NonNull
  Optional<DseEdgeMetadata> getEdge();

  @NonNull
  @Override
  default String describe(boolean pretty) {
    return DseTableMetadata.super.describe(pretty);
  }
}
