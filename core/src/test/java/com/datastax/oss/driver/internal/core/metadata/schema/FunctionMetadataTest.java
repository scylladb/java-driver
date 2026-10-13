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
package com.datastax.oss.driver.internal.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.metadata.schema.DseFunctionMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class FunctionMetadataTest {

  @Test
  public void should_escape_function_body() {
    assertBodyEscaped(FunctionMetadata.class);
  }

  @Test
  public void should_escape_legacy_dse_function_body() {
    assertBodyEscaped(DseFunctionMetadata.class);
  }

  private void assertBodyEscaped(Class<? extends FunctionMetadata> metadataType) {
    FunctionMetadata function = mock(metadataType, CALLS_REAL_METHODS);
    when(function.getKeyspace()).thenReturn(CqlIdentifier.fromCql("ks"));
    when(function.getSignature()).thenReturn(new FunctionSignature("fn"));
    when(function.getReturnType()).thenReturn(DataTypes.TEXT);
    when(function.getLanguage()).thenReturn("java");
    when(function.getBody()).thenReturn("return \"O'Reilly $$\";");
    if (function instanceof DseFunctionMetadata) {
      doReturn(Optional.empty()).when((DseFunctionMetadata) function).getMonotonicity();
    }

    String expected = "AS 'return \"O''Reilly $$\";';";
    assertThat(function.describe(false)).endsWith(expected);
    assertThat(function.describe(true)).endsWith(expected);
    assertThat(function.describeWithChildren(false)).endsWith(expected);
  }
}
