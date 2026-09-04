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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;

/**
 * A caller-supplied step that runs against every candidate channel a {@link ChannelFactory#connect}
 * attempt opens, after protocol initialization succeeds and before the attempt is considered
 * successful.
 *
 * <p>Its position is what makes it useful: a connect attempt may try several addresses when the
 * endpoint's name resolves to more than one, and the hook runs while the factory still holds the
 * remaining ones. Completing the returned stage exceptionally (or throwing synchronously) rejects
 * the candidate -- the factory closes the channel and moves on to the endpoint's next address -- so
 * a caller can impose its own acceptance criteria on a channel, per address, without losing the
 * fallback. The control connection uses this to read {@code system.local} and refuse a channel
 * whose node cannot identify itself, channeling what it read straight into its own state (see
 * {@code ControlConnection}).
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>invoked at most once per candidate channel, and candidates are tried serially -- but a hook
 *       that has not completed by {@link DriverChannelOptions#connectHookTimeout} is <b>abandoned,
 *       not cancelled</b>. The factory rejects that candidate and calls the hook for the next
 *       address while the stranded stage is still outstanding, so two invocations from one connect
 *       attempt can be live at once, and a late one can complete after its own candidate has been
 *       closed. An implementation that carries state between the hook and the rest of the attempt
 *       must therefore publish it per channel and atomically, rather than assume the previous
 *       invocation has finished -- which is what {@code ControlConnection.NodeInfoHolder} does, and
 *       why it can;
 *   <li>invoked on the channel's event loop: implementations must not block, and anything heavier
 *       than an asynchronous request on the channel itself should hop to another thread;
 *   <li>the returned stage must eventually complete; the factory bounds it with {@link
 *       DriverChannelOptions#connectHookTimeout} and rejects the candidate when it expires;
 *   <li>only channel-scoped resources may be touched: the channel is not published to the caller
 *       yet, and a rejected or timed-out candidate is closed by the factory.
 * </ul>
 *
 * <p>When the options also request protocol events ({@link DriverChannelOptions#eventTypes}), the
 * {@code REGISTER} request is sent after the hook completes successfully, so a channel that is
 * about to be rejected never registers for events.
 */
public interface ConnectHook {

  /**
   * Vets a candidate channel that completed protocol initialization.
   *
   * @return a stage that completes normally to accept the channel, or exceptionally to reject it
   *     and make the connect attempt move on to the endpoint's next address.
   */
  @NonNull
  CompletionStage<Void> onConnect(@NonNull DriverChannel channel);
}
