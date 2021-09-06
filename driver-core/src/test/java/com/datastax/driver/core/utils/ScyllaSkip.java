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

package com.datastax.driver.core.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation for a Class or Method that skips a test when testing with Scylla.
 *
 * @see com.datastax.driver.core.TestListener#beforeInvocation(org.testng.IInvokedMethod,
 *     org.testng.ITestResult)
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ScyllaSkip {
  /** @return The description returned if the Scylla requirement is not met. */
  String description() default "Marked skip when testing with Scylla.";
}
