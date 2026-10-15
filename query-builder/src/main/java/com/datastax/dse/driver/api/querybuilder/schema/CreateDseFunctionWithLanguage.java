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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Legacy DSE schema builder stage retained for compatibility.
 *
 * @deprecated DSE schema construction is no longer supported. This type remains for source and
 *     binary compatibility.
 */
@Deprecated
public interface CreateDseFunctionWithLanguage {

  /**
   * Adds AS to the create function specification. This is used to specify the body of the function.
   * Note that it is expected that the provided body is properly quoted as this method does not make
   * that decision for the user. For simple cases, one should wrap the input in single quotes, i.e.
   * <code>'myBody'</code>. If the body itself contains single quotes, one could use a
   * postgres-style string literal, which is surrounded in two dollar signs, i.e. <code>$$ myBody $$
   * </code>.
   */
  @NonNull
  CreateDseFunctionEnd as(@NonNull String functionBody);

  /**
   * Adds AS to the create function specification and quotes the function body. Bodies containing
   * single quotes use dollar quoting unless they also contain the {@code $$} delimiter. Otherwise,
   * the body is enclosed in single quotes and embedded single quotes are escaped.
   *
   * <p>If the function body is already quoted, use {@link #as(String)} instead.
   */
  @NonNull
  default CreateDseFunctionEnd asQuoted(@NonNull String functionBody) {
    return as(CqlHelper.quoteFunctionBody(functionBody));
  }
}
