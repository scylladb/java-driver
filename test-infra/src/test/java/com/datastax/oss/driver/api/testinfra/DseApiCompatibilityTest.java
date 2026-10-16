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
package com.datastax.oss.driver.api.testinfra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.CqlSessionRuleBuilder;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class DseApiCompatibilityTest {

  @Test
  public void should_report_no_dse_version_for_supported_backends() {
    assertThat(CcmBridge.builder().build().getDseVersion()).isEmpty();
  }

  @Test
  public void should_reject_dse_before_discovering_backend_version() throws Exception {
    String[] entries = System.getProperty("java.class.path").split(File.pathSeparator);
    URL[] classpath = new URL[entries.length];
    for (int i = 0; i < entries.length; i++) {
      classpath[i] = new File(entries[i]).toURI().toURL();
    }
    Properties originalProperties = System.getProperties();
    for (String property : new String[] {"ccm.distribution", "ccm.dse"}) {
      Properties properties = new Properties();
      properties.putAll(originalProperties);
      properties.setProperty("ccm.distribution", "CASSANDRA");
      properties.setProperty("ccm.dse", "false");
      properties.setProperty(property, property.equals("ccm.dse") ? "true" : "DSE");
      properties.setProperty("ccm.version", "not-a-version");
      System.setProperties(properties);
      try (URLClassLoader loader =
          new URLClassLoader(classpath, ClassLoader.getPlatformClassLoader())) {
        assertThatThrownBy(() -> Class.forName(CcmBridge.class.getName(), true, loader))
            .isInstanceOf(ExceptionInInitializerError.class)
            .hasCauseInstanceOf(UnsupportedOperationException.class);
      } finally {
        System.setProperties(originalProperties);
      }
    }
  }

  @Test
  public void should_keep_legacy_backend_names_and_reject_dse_setup() {
    assertThat(BackendType.DSE.getFriendlyName()).isEqualTo("DSE");
    assertThatThrownBy(() -> CcmBridge.builder().withDseWorkloads("Graph"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new CqlSessionRuleBuilder(null).withCreateGraph())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
