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
package com.datastax.oss.driver.internal.core.channel;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.local.LocalAddress;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A stand-in for a user-supplied {@code AddressResolverGroup} (e.g. Netty's {@code
 * DnsAddressResolverGroup}), installed the way a user would install one: through {@link
 * com.datastax.oss.driver.internal.core.context.NettyOptions#afterBootstrapInitialized(Bootstrap)}.
 *
 * <p>Records what it was asked to resolve, and answers with a fixed list of addresses so tests can
 * assert that <i>every</i> one of them is tried, and in what order.
 *
 * <p>Implements {@link AddressResolver} directly rather than extending {@code
 * AbstractAddressResolver} so it can hand back {@link LocalAddress}es — the unit tests connect over
 * Netty's local transport, which is not reachable through an {@link InetSocketAddress}.
 */
class TestAddressResolverGroup extends AddressResolverGroup<SocketAddress> {

  /** Every address this group was asked to resolve, in order. */
  final List<SocketAddress> queried = new CopyOnWriteArrayList<>();

  /** Whether a resolver was ever obtained from this group at all. */
  volatile boolean resolverRequested;

  /** The executor the last resolver was created for, i.e. the loop resolution runs on. */
  @Nullable volatile EventExecutor resolverExecutor;

  /** The addresses to answer with, or {@code null} to fail every lookup. */
  @Nullable private final List<SocketAddress> answer;

  /**
   * Whether to claim that every address still needs resolving, even one that already carries an IP.
   * A real resolver may do this to redirect traffic, and Netty honours it: {@code
   * Bootstrap#doResolveAndConnect0} asks the resolver rather than testing the address itself.
   */
  private final boolean claimNothingIsResolved;

  TestAddressResolverGroup(@Nullable List<SocketAddress> answer) {
    this(answer, false);
  }

  TestAddressResolverGroup(@Nullable List<SocketAddress> answer, boolean claimNothingIsResolved) {
    this.answer = answer;
    this.claimNothingIsResolved = claimNothingIsResolved;
  }

  @Override
  protected AddressResolver<SocketAddress> newResolver(EventExecutor executor) {
    resolverRequested = true;
    resolverExecutor = executor;
    return new AddressResolver<SocketAddress>() {

      @Override
      public boolean isSupported(SocketAddress address) {
        return true;
      }

      @Override
      public boolean isResolved(SocketAddress address) {
        if (claimNothingIsResolved) {
          return false;
        }
        // Only hostnames need resolving; anything else (including the local-transport addresses we
        // hand back) is already usable.
        return !(address instanceof InetSocketAddress)
            || !((InetSocketAddress) address).isUnresolved();
      }

      @Override
      public Future<SocketAddress> resolve(SocketAddress address) {
        return resolve(address, executor.newPromise());
      }

      @Override
      public Future<SocketAddress> resolve(SocketAddress address, Promise<SocketAddress> promise) {
        queried.add(address);
        return answer == null
            ? promise.setFailure(new IllegalStateException("mock resolver failure"))
            : promise.setSuccess(answer.get(0));
      }

      @Override
      public Future<List<SocketAddress>> resolveAll(SocketAddress address) {
        return resolveAll(address, executor.newPromise());
      }

      @Override
      public Future<List<SocketAddress>> resolveAll(
          SocketAddress address, Promise<List<SocketAddress>> promise) {
        queried.add(address);
        return answer == null
            ? promise.setFailure(new IllegalStateException("mock resolver failure"))
            : promise.setSuccess(answer);
      }

      @Override
      public void close() {
        // nothing to do
      }
    };
  }
}
