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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * A builder to create a script graph statement.
 *
 * <p>This class is mutable and not thread-safe.
 *
 * @deprecated DSE Graph is not supported starting with driver 4.19.2.2.
 */
@NotThreadSafe
@SuppressWarnings("DoNotCallSuggester")
@Deprecated
public class ScriptGraphStatementBuilder
    extends GraphStatementBuilderBase<ScriptGraphStatementBuilder, ScriptGraphStatement> {

  public ScriptGraphStatementBuilder() {
    throw GraphSupportRemoved.exception();
  }

  public ScriptGraphStatementBuilder(String script) {
    throw GraphSupportRemoved.exception();
  }

  public ScriptGraphStatementBuilder(ScriptGraphStatement template) {
    super(template);
    throw GraphSupportRemoved.exception();
  }

  @NonNull
  public ScriptGraphStatementBuilder setScript(@NonNull String script) {
    throw GraphSupportRemoved.exception();
  }

  /** @see ScriptGraphStatement#isSystemQuery() */
  @NonNull
  public ScriptGraphStatementBuilder setSystemQuery(@Nullable Boolean isSystemQuery) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Set a value for a parameter defined in the script query.
   *
   * @see ScriptGraphStatement#setQueryParam(String, Object)
   */
  @NonNull
  public ScriptGraphStatementBuilder setQueryParam(@NonNull String name, @Nullable Object value) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Set multiple values for named parameters defined in the script query.
   *
   * @see ScriptGraphStatement#setQueryParam(String, Object)
   */
  @NonNull
  public ScriptGraphStatementBuilder setQueryParams(@NonNull Map<String, Object> params) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Removes a parameter.
   *
   * <p>This is useful if the builder was {@linkplain
   * ScriptGraphStatement#builder(ScriptGraphStatement) initialized with a template statement} that
   * has more parameters than desired.
   *
   * @see ScriptGraphStatement#setQueryParam(String, Object)
   * @see #clearQueryParams()
   */
  @NonNull
  public ScriptGraphStatementBuilder removeQueryParam(@NonNull String name) {
    throw GraphSupportRemoved.exception();
  }

  /** Clears all the parameters previously added to this builder. */
  public ScriptGraphStatementBuilder clearQueryParams() {
    throw GraphSupportRemoved.exception();
  }

  @NonNull
  @Override
  public ScriptGraphStatement build() {
    throw GraphSupportRemoved.exception();
  }
}
