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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class Parameters {
  private final ConsistencyLevel defaultConsistency;
  private final ConsistencyLevel defaultSerialConsistency;

  @JsonCreator
  public Parameters(
      @JsonProperty(value = "defaultConsistency") ConsistencyLevel defaultConsistency,
      @JsonProperty(value = "defaultSerialConsistency") ConsistencyLevel defaultSerialConsistency) {
    this.defaultConsistency = defaultConsistency;
    this.defaultSerialConsistency = defaultSerialConsistency;
  }
}
