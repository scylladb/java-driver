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
package com.datastax.dse.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.ThreadSafe;

/**
 * A server-side error triggered when DSE can't send asynchronous results back to the client.
 *
 * <p>This type is retained for compatibility. The driver no longer executes DSE continuous paging
 * requests and does not produce this error.
 *
 * <p>Note that the protocol specification refers to this error as {@code CLIENT_WRITE_FAILURE}; we
 * don't follow that terminology because it would be too misleading (this is not a client error, and
 * it doesn't occur while writing data to DSE).
 *
 * @deprecated DSE continuous paging is no longer supported.
 */
@Deprecated
@ThreadSafe
public class UnfitClientException extends CoordinatorException {

  public UnfitClientException(@NonNull Node coordinator, @NonNull String message) {
    this(coordinator, message, null, false);
  }

  private UnfitClientException(
      @NonNull Node coordinator,
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(coordinator, message, executionInfo, writableStackTrace);
  }

  @Override
  @NonNull
  public UnfitClientException copy() {
    return new UnfitClientException(getCoordinator(), getMessage(), getExecutionInfo(), true);
  }
}
