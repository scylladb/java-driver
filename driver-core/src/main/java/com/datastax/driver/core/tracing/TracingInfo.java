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

/**
 * An abstraction layer over instrumentation library API, corresponding to a logical span in the
 * trace.
 */
public interface TracingInfo {

  /** Final status of the traced execution. */
  enum StatusCode {
    OK,
    ERROR,
  }

  /**
   * Starts a span corresponding to this {@link TracingInfo} object. Must be called exactly once,
   * before any other method, at the beginning of the traced execution.
   *
   * @param name the name given to the span being created.
   */
  void setNameAndStartTime(String name);

  /**
   * Records in the trace that the provided exception occured.
   *
   * @param exception the exception to be recorded.
   */
  void recordException(Exception exception);

  /**
   * Sets the final status of the traced execution.
   *
   * @param code the status code to be set.
   */
  void setStatus(StatusCode code);

  /**
   * Sets the final status of the traced execution, with additional description.
   *
   * @param code the status code to be set.
   * @param description the additional description of the status.
   */
  void setStatus(StatusCode code, String description);

  /** Must be always called exactly once at the logical end of traced execution. */
  void tracingFinished();
}
