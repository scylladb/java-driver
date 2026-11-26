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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.Test;

public class CanonicalNodeListBuilderTest {

  @Test
  public void should_canonicalize_equal_lists_without_conflating_hash_collisions() {
    InternalDriverContext context = MockedDriverContextFactory.defaultDriverContext();
    Node first = new CollidingNode(9042, context);
    Node second = new CollidingNode(9043, context);
    CanonicalNodeListBuilder builder = new CanonicalNodeListBuilder();

    builder.add(first);
    List<Node> firstList = builder.build();

    builder.clear();
    builder.add(second);
    assertThat(builder.build()).containsExactly(second);

    builder.clear();
    builder.add(first);
    assertThat(builder.build()).isSameAs(firstList);
  }

  private static class CollidingNode extends DefaultNode {
    CollidingNode(int port, InternalDriverContext context) {
      super(new DefaultEndPoint(new InetSocketAddress("localhost", port)), context);
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }
}
