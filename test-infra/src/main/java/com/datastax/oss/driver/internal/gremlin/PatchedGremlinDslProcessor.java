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
package com.datastax.oss.driver.internal.gremlin;

import java.util.Collections;
import java.util.Set;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDslProcessor;

@SupportedAnnotationTypes("org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl")
public class PatchedGremlinDslProcessor extends GremlinDslProcessor {

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    return Collections.singleton("org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl");
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latestSupported();
  }
}
