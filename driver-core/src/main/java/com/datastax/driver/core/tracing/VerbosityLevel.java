/*
 * Copyright (C) 2021 ScyllaDB
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

package com.datastax.driver.core.tracing;

/** The verbosity level of tracing data that is to be collected. May be extended in the future. */
public enum VerbosityLevel {
  // Enum elements must be listed in ascending order of verbosity level (i.e. each next element
  // listed adds some new information).
  NORMAL,
  FULL;
}
