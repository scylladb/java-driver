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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface ReplicationStrategy {

  /**
   * Computes the replicas for each token in the ring.
   *
   * @param tokenToPrimary mapping from token to its primary replica node
   * @param ring ordered list of tokens representing the ring
   * @return a map where keys are tokens and values are the sets of replica nodes for each token
   * @deprecated Use {@link #computeReplicasListByToken(Map, List)} instead.
   */
  @Deprecated
  default Map<Token, Set<Node>> computeReplicasByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring) {
    return computeReplicasListByToken(tokenToPrimary, ring).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())));
  }

  /**
   * Computes the replicas for each token in the ring.
   *
   * @param tokenToPrimary mapping from token to its primary replica node
   * @param ring ordered list of tokens representing the ring
   * @return a map where keys are tokens and values are the ordered lists of replica nodes for each
   *     token
   */
  Map<Token, List<Node>> computeReplicasListByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring);
}
