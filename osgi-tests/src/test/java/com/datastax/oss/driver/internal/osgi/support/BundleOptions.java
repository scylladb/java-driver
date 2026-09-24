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
package com.datastax.oss.driver.internal.osgi.support;

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.CoreOptions.systemTimeout;
import static org.ops4j.pax.exam.CoreOptions.vmOption;

import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;

public class BundleOptions {

  public static CompositeOption commonBundles() {
    return () ->
        options(
            mavenBundle("org.apache.cassandra", "java-driver-guava-shaded").versionAsInProject(),
            mavenBundle("io.dropwizard.metrics", "metrics-core").versionAsInProject(),
            mavenBundle("org.slf4j", "slf4j-api").versionAsInProject(),
            mavenBundle("org.hdrhistogram", "HdrHistogram").versionAsInProject(),
            mavenBundle("com.typesafe", "config").versionAsInProject(),
            mavenBundle("com.scylladb", "native-protocol").versionAsInProject(),
            logbackBundles(),
            debugOptions());
  }

  public static CompositeOption applicationBundle() {
    return () ->
        options(
            systemProperty("cassandra.contactpoints").value("127.0.0.1"),
            systemProperty("cassandra.port").value("9042"),
            systemProperty("cassandra.keyspace").value("test_osgi"),
            bundle("reference:file:target/classes"));
  }

  public static UrlProvisionOption driverCoreBundle() {
    return bundle("reference:file:../core/target/classes");
  }

  public static UrlProvisionOption driverCoreShadedBundle() {
    return bundle("reference:file:../core-shaded/target/classes");
  }

  public static UrlProvisionOption driverQueryBuilderBundle() {
    return bundle("reference:file:../query-builder/target/classes");
  }

  public static UrlProvisionOption driverMapperRuntimeBundle() {
    return bundle("reference:file:../mapper-runtime/target/classes");
  }

  public static UrlProvisionOption driverTestInfraBundle() {
    return bundle("reference:file:../test-infra/target/classes");
  }

  public static CompositeOption testBundles() {
    return () ->
        options(
            driverTestInfraBundle(),
            mavenBundle("org.apache.commons", "commons-exec").versionAsInProject(),
            mavenBundle("org.assertj", "assertj-core").versionAsInProject(),
            mavenBundle("org.awaitility", "awaitility").versionAsInProject(),
            mavenBundle("org.hamcrest", "hamcrest").versionAsInProject(),
            junitBundles());
  }

  public static CompositeOption nettyBundles() {
    return () ->
        options(
            mavenBundle("io.netty", "netty-handler").versionAsInProject(),
            mavenBundle("io.netty", "netty-buffer").versionAsInProject(),
            mavenBundle("io.netty", "netty-codec").versionAsInProject(),
            mavenBundle("io.netty", "netty-common").versionAsInProject(),
            mavenBundle("io.netty", "netty-transport").versionAsInProject(),
            mavenBundle("io.netty", "netty-transport-native-unix-common").versionAsInProject(),
            mavenBundle("io.netty", "netty-resolver").versionAsInProject());
  }

  public static CompositeOption logbackBundles() {
    return () ->
        options(
            mavenBundle("ch.qos.logback", "logback-classic").versionAsInProject(),
            mavenBundle("ch.qos.logback", "logback-core").versionAsInProject(),
            systemProperty("logback.configurationFile")
                .value("file:src/test/resources/logback-test.xml"));
  }

  public static CompositeOption jacksonBundles() {
    return () ->
        options(
            mavenBundle("com.fasterxml.jackson.core", "jackson-databind").versionAsInProject(),
            mavenBundle("com.fasterxml.jackson.core", "jackson-core").versionAsInProject(),
            mavenBundle("com.fasterxml.jackson.core", "jackson-annotations").versionAsInProject());
  }

  public static CompositeOption lz4Bundle() {
    return () ->
        options(
            mavenBundle("at.yawk.lz4", "lz4-java").versionAsInProject(),
            systemProperty("cassandra.compression").value("LZ4"));
  }

  public static CompositeOption snappyBundle() {
    return () ->
        options(
            mavenBundle("org.xerial.snappy", "snappy-java").versionAsInProject(),
            systemProperty("cassandra.compression").value("SNAPPY"));
  }

  public static CompositeOption esriBundles() {
    return () ->
        options(
            CoreOptions.wrappedBundle(
                    mavenBundle("com.esri.geometry", "esri-geometry-api").versionAsInProject())
                .exports("com.esri.core.geometry.*")
                .imports("org.json", "org.codehaus.jackson")
                .bundleSymbolicName("com.esri.core.geometry")
                .overwriteManifest(WrappedUrlProvisionOption.OverwriteMode.FULL),
            mavenBundle("org.json", "json").versionAsInProject(),
            mavenBundle("org.codehaus.jackson", "jackson-core-asl").versionAsInProject(),
            systemProperty("cassandra.geo").value("true"));
  }

  public static CompositeOption reactiveBundles() {
    return () ->
        options(
            mavenBundle("org.reactivestreams", "reactive-streams").versionAsInProject(),
            mavenBundle("io.reactivex.rxjava2", "rxjava").versionAsInProject(),
            systemProperty("cassandra.reactive").value("true"));
  }

  private static CompositeOption debugOptions() {
    boolean debug = Boolean.getBoolean("osgi.debug");
    if (debug) {
      return () ->
          options(
              vmOption("-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"),
              systemTimeout(Long.MAX_VALUE));
    } else {
      return CoreOptions::options;
    }
  }
}
