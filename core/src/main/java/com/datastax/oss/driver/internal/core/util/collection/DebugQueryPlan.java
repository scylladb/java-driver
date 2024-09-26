/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.util.collection;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.LazyCopyQueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;
import net.jcip.annotations.ThreadSafe;

/** A query plan that attaches debug information to original query plan. */
@ThreadSafe
public class DebugQueryPlan extends AbstractQueue<Node> implements QueryPlan {
  private final Queue<Node> plan;
  private Queue<Node> localPlan;
  private Serializable policyInfo;
  private Serializable policyWrapperInfo;

  public DebugQueryPlan(@NonNull Queue<Node> originalPlan) {
    if (originalPlan instanceof LazyCopyQueryPlan) {
      this.plan = originalPlan;
    } else {
      this.plan = new LazyCopyQueryPlan(originalPlan);
    }
  }

  @Override
  public Iterator<Node> iterator() {
    return this.plan.iterator();
  }

  @Override
  public int size() {
    return this.plan.size();
  }

  public QueryPlan setLocalPlan(@NonNull QueryPlan plan) {
    LazyCopyQueryPlan copyPlan = new LazyCopyQueryPlan(plan);
    this.localPlan = copyPlan;
    return copyPlan;
  }

  public void setLoadBalancingPolicyInfo(Serializable policyInfo) {
    this.policyInfo = policyInfo;
  }

  public void setLoadBalancingPolicyWrapperInfo(Serializable policyWrapperInfo) {
    this.policyWrapperInfo = policyWrapperInfo;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(this.getClass().getName()).append("(plan: ").append(plan);
    if (this.policyInfo != null) {
      result.append(", policy: ").append(policyInfo);
    }
    if (this.policyWrapperInfo != null) {
      result.append(", wrapper: ").append(policyWrapperInfo);
    }
    if (this.localPlan != null) {
      result.append(", localPlan: ").append(localPlan);
    }
    result.append(")");
    return result.toString();
  }

  @Override
  public Node poll() {
    return plan.poll();
  }
}
