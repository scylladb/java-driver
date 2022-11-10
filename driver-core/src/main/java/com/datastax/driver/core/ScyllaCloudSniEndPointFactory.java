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
/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.UUID;

class ScyllaCloudSniEndPointFactory implements EndPointFactory {
  private final String nodeDomain;

  private final InetSocketAddress proxy;

  public ScyllaCloudSniEndPointFactory(InetSocketAddress proxy, String nodeDomain) {
    this.proxy = proxy;
    this.nodeDomain = nodeDomain;
  }

  @Override
  public void init(Cluster cluster) {}

  @Override
  public EndPoint create(Row row) {
    UUID host_id = row.getUUID("host_id");
    String sni = host_id.toString() + "." + nodeDomain;
    return new SniEndPoint(proxy, sni);
  }
}
