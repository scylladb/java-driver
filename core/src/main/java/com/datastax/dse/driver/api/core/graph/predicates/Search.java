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
package com.datastax.dse.driver.api.core.graph.predicates;

import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/** @deprecated DSE Graph is not supported starting with driver 4.19.2.2. */
@SuppressWarnings("DoNotCallSuggester")
@Deprecated
public interface Search {

  /**
   * Search any instance of a certain token within the text property targeted (case insensitive).
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> token(String value) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Search any instance of a certain token prefix within the text property targeted (case
   * insensitive).
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> tokenPrefix(String value) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Search any instance of the provided regular expression for the targeted property (case
   * insensitive).
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> tokenRegex(String value) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Search for a specific prefix at the beginning of the text property targeted (case sensitive).
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> prefix(String value) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Search for this regular expression inside the text property targeted (case sensitive).
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> regex(String value) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Supports finding words which are a within a specific distance away (case insensitive).
   *
   * <p>Example: the search expression is {@code phrase("Hello world", 2)}
   *
   * <ul>
   *   <li>the inserted value "Hello world" is found
   *   <li>the inserted value "Hello wild world" is found
   *   <li>the inserted value "Hello big wild world" is found
   *   <li>the inserted value "Hello the big wild world" is not found
   *   <li>the inserted value "Goodbye world" is not found.
   * </ul>
   *
   * @param query the string to look for in the value
   * @param distance the number of terms allowed between two correct terms to find a value.
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> phrase(String query, int distance) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Supports fuzzy searches based on the Damerau-Levenshtein Distance, or Edit Distance algorithm
   * (case sensitive).
   *
   * <p>Example: the search expression is {@code fuzzy("david", 1)}
   *
   * <ul>
   *   <li>the inserted value "david" is found
   *   <li>the inserted value "dawid" is found
   *   <li>the inserted value "davids" is found
   *   <li>the inserted value "dewid" is not found
   * </ul>
   *
   * @param query the string to look for in the value
   * @param distance the number of "uncertainties" allowed for the Levenshtein algorithm.
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> fuzzy(String query, int distance) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Supports fuzzy searches based on the Damerau-Levenshtein Distance, or Edit Distance algorithm
   * after having tokenized the data stored (case insensitive).
   *
   * <p>Example: the search expression is {@code tokenFuzzy("david", 1)}
   *
   * <ul>
   *   <li>the inserted value "david" is found
   *   <li>the inserted value "dawid" is found
   *   <li>the inserted value "hello-dawid" is found
   *   <li>the inserted value "dewid" is not found
   * </ul>
   *
   * @param query the string to look for in the value
   * @param distance the number of "uncertainties" allowed for the Levenshtein algorithm.
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> tokenFuzzy(String query, int distance) {
    throw GraphSupportRemoved.exception();
  }
}
