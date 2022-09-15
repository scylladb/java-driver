/*
 * Copyright (C) 2022 ScyllaDB
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
package com.datastax.driver.core;

import java.net.InetSocketAddress;

class MockSniEndPointFactory implements EndPointFactory {
  private final InetSocketAddress proxyAddress;
  private final EndPointFactory childSniFactory;

  public MockSniEndPointFactory(InetSocketAddress proxyAddress, EndPointFactory childSniFactory) {
    this.proxyAddress = proxyAddress;
    this.childSniFactory = childSniFactory;
  }

  @Override
  public void init(Cluster cluster) {
    childSniFactory.init(cluster);
  }

  @Override
  public EndPoint create(Row peersRow) {
    SniEndPoint originalEndPoint = (SniEndPoint) childSniFactory.create(peersRow);
    return new SniEndPoint(proxyAddress, originalEndPoint.getServerName());
  }
}
