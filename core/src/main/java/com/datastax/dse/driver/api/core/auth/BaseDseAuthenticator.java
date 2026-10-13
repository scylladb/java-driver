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
package com.datastax.dse.driver.api.core.auth;

import com.datastax.oss.driver.api.core.auth.SyncAuthenticator;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;

/**
 * Base class retained for ordinary authentication by existing subclasses.
 *
 * @deprecated DSE authentication negotiation is no longer supported.
 */
@Deprecated
@ThreadSafe
public abstract class BaseDseAuthenticator implements SyncAuthenticator {

  protected BaseDseAuthenticator(@NonNull String serverAuthenticator) {
    if (Objects.requireNonNull(serverAuthenticator)
        .equals("com.datastax.bdp.cassandra.auth.DseAuthenticator")) {
      throw new UnsupportedOperationException(
          "DSE authentication negotiation is no longer supported");
    }
  }

  @NonNull
  protected abstract ByteBuffer getMechanism();

  @NonNull
  protected abstract ByteBuffer getInitialServerChallenge();

  @Nullable
  @Override
  public ByteBuffer initialResponseSync() {
    return evaluateChallengeSync(getInitialServerChallenge());
  }

  @Override
  public void onAuthenticationSuccessSync(@Nullable ByteBuffer token) {}
}
