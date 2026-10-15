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

import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;

/** @deprecated DSE proxy execution is no longer supported. */
@Deprecated
@SuppressWarnings("DoNotCallSuggester")
public class ProxyAuthentication {

  /** @deprecated DSE proxy execution is no longer supported. */
  @Deprecated
  @NonNull
  public static <StatementT extends Statement<StatementT>> StatementT executeAs(
      @NonNull String userOrRole, @NonNull StatementT statement) {
    throw new UnsupportedOperationException("DSE proxy execution is no longer supported");
  }
}
