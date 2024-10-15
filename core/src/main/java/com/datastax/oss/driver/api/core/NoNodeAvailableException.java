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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Queue;

/**
 * Specialization of {@code AllNodesFailedException} when no coordinators were tried.
 *
 * <p>This can happen if all nodes are down, or if all the contact points provided at startup were
 * invalid.
 */
public class NoNodeAvailableException extends AllNodesFailedException {
  public NoNodeAvailableException() {
    this(null);
  }

  private NoNodeAvailableException(
      String message, ExecutionInfo executionInfo, Queue<Node> queryPlan) {
    super(message, executionInfo, Collections.emptySet(), queryPlan);
  }

  public NoNodeAvailableException(Queue<Node> queryPlan) {
    this(buildMessage(queryPlan), null, queryPlan);
  }

  private static String buildMessage(Queue<Node> queryPlan) {
    if (queryPlan == null) {
      return "No node was available to execute the query";
    }
    return "No node was available to execute the query. Query Plan: " + queryPlan;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new NoNodeAvailableException(getMessage(), getExecutionInfo(), getQueryPlan());
  }
}
