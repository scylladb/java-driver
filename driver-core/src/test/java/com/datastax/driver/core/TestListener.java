/*
 * Copyright DataStax, Inc.
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

/*
 * Copyright (C) 2021 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.core.utils.DseVersion;
import com.datastax.driver.core.utils.ScyllaOnly;
import com.datastax.driver.core.utils.ScyllaSkip;
import com.datastax.driver.core.utils.ScyllaVersion;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.TestListenerAdapter;
import org.testng.internal.ConstructorOrMethod;

public class TestListener extends TestListenerAdapter implements IInvokedMethodListener {

  private long start_time = System.nanoTime();
  private int test_index = 0;

  @Override
  public void onTestFailure(ITestResult tr) {
    long elapsedTime = TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start_time));
    long testTime = tr.getEndMillis() - tr.getStartMillis();
    tr.getThrowable().printStackTrace();
    System.out.println("FAILED : " + tr.getName());
    System.out.println("Test   : " + formatIntoHHMMSS(testTime / 1000));
    System.out.println("Elapsed: " + formatIntoHHMMSS(elapsedTime));
    System.out.println();
  }

  @Override
  public void onTestSkipped(ITestResult tr) {
    long elapsedTime = TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start_time));
    long testTime = tr.getEndMillis() - tr.getStartMillis();
    String skipReason =
        tr.getThrowable() != null ? " (" + tr.getThrowable().getMessage() + ")" : "";
    System.out.println("SKIPPED: " + tr.getName() + skipReason);
    System.out.println("Test   : " + formatIntoHHMMSS(testTime / 1000));
    System.out.println("Elapsed: " + formatIntoHHMMSS(elapsedTime));
    System.out.println();
  }

  @Override
  public void onTestSuccess(ITestResult tr) {
    long elapsedTime = TimeUnit.NANOSECONDS.toSeconds((System.nanoTime() - start_time));
    long testTime = tr.getEndMillis() - tr.getStartMillis();
    System.out.println("SUCCESS: " + tr.getName());
    System.out.println("Test   : " + formatIntoHHMMSS(testTime / 1000));
    System.out.println("Elapsed: " + formatIntoHHMMSS(elapsedTime));
    System.out.println();
  }

  @Override
  public void onTestStart(ITestResult tr) {
    System.out.println();
    System.out.println("-----------------------------------------------");
    System.out.println(
        "Starting "
            + tr.getTestClass().getName()
            + '.'
            + tr.getName()
            + " [Test #"
            + ++test_index
            + "]...");
  }

  static String formatIntoHHMMSS(long secondsTotal) {
    long hours = secondsTotal / 3600,
        remainder = secondsTotal % 3600,
        minutes = remainder / 60,
        seconds = remainder % 60;

    return ((hours < 10 ? "0" : "")
        + hours
        + ':'
        + (minutes < 10 ? "0" : "")
        + minutes
        + ':'
        + (seconds < 10 ? "0" : "")
        + seconds);
  }

  @Override
  public void beforeInvocation(IInvokedMethod testMethod, ITestResult testResult) {
    // Check to see if the class or method is annotated with 'CassandraVersion', if so ensure the
    // version we are testing with meets the requirement, if not a SkipException is thrown
    // and this test is skipped.
    ITestNGMethod testNgMethod = testResult.getMethod();
    ConstructorOrMethod constructorOrMethod = testNgMethod.getConstructorOrMethod();

    try {
      Class<?> clazz = testNgMethod.getInstance().getClass();
      do {
        // Check for skip conditions and break early if version annotations are found
        if (checkForSkipConditions(clazz)) break;
      } while (!(clazz = clazz.getSuperclass()).equals(Object.class));
      Method method = constructorOrMethod.getMethod();
      if (method != null) {
        checkForSkipConditions(method); // Don't need return value for methods
      }
    } catch (SkipException e) {
      // Workaround for testng 6.13.x bug https://github.com/testng-team/testng/issues/1632
      // When SkipException thrown from beforeInvocation marks test as FAILED
      // Instead of letting TestNG handle it, we manually set the skip status
      testResult.setStatus(ITestResult.SKIP);
      testResult.setThrowable(e);
      testResult.setEndMillis(System.currentTimeMillis());
      // Don't re-throw the exception to avoid the bug
    }
  }

  /**
   * Static method to check for skip conditions on a class or method. Throws SkipException if the
   * element should be skipped.
   *
   * @return true if version-related annotations were found (to break early in class hierarchy scan)
   */
  public static boolean checkForSkipConditions(AnnotatedElement element) {
    if (CCMBridge.getGlobalScyllaVersion() != null) {
      if (element.isAnnotationPresent(ScyllaSkip.class)) {
        throw new SkipException("Skipping test because it is disabled for Scylla cluster.");
      }

      if (element.isAnnotationPresent(ScyllaVersion.class)) {
        ScyllaVersion scyllaVersion = element.getAnnotation(ScyllaVersion.class);
        scyllaVersionCheck(scyllaVersion);
        return true;
      }

      if (element.isAnnotationPresent(ScyllaOnly.class)) {
        return true;
      }

      if (element.isAnnotationPresent(CassandraVersion.class)) {
        CassandraVersion cassandraVersion = element.getAnnotation(CassandraVersion.class);
        cassandraVersionCheck(cassandraVersion);
        return true;
      }

      if (element.isAnnotationPresent(DseVersion.class)) {
        throw new SkipException(
            "Skipping test because it is designed for DSE only, but running on Scylla cluster.");
      }
    } else if (CCMBridge.isDse()) {
      if (element.isAnnotationPresent(ScyllaOnly.class)) {
        throw new SkipException("Skipping test because it is enabled only for Scylla cluster.");
      }

      if (element.isAnnotationPresent(DseVersion.class)) {
        DseVersion dseVersion = element.getAnnotation(DseVersion.class);
        dseVersionCheck(dseVersion);
        return true;
      }

      if (element.isAnnotationPresent(CassandraVersion.class)) {
        CassandraVersion cassandraVersion = element.getAnnotation(CassandraVersion.class);
        cassandraVersionCheck(cassandraVersion);
        return true;
      }

      if (element.isAnnotationPresent(ScyllaVersion.class)) {
        throw new SkipException(
            "Skipping test because it is designed for Scylla only, but running on DSE cluster.");
      }
    } else {
      if (element.isAnnotationPresent(ScyllaOnly.class)) {
        throw new SkipException("Skipping test because it is enabled only for Scylla cluster.");
      }

      if (element.isAnnotationPresent(CassandraVersion.class)) {
        CassandraVersion cassandraVersion = element.getAnnotation(CassandraVersion.class);
        cassandraVersionCheck(cassandraVersion);
        return true;
      }

      if (element.isAnnotationPresent(ScyllaVersion.class)) {
        throw new SkipException(
            "Skipping test because it is designed for Scylla only, but running on Cassandra cluster.");
      }

      if (element.isAnnotationPresent(DseVersion.class)) {
        throw new SkipException(
            "Skipping test because it is designed for DSE only, but running on Cassandra cluster.");
      }
    }
    return false; // No version annotations found, continue scanning
  }

  @Override
  public void afterInvocation(IInvokedMethod testMethod, ITestResult testResult) {
    // Do nothing
  }

  private static void cassandraVersionCheck(CassandraVersion version) {
    versionCheck(
        CCMBridge.getGlobalCassandraVersion(),
        VersionNumber.parse(version.value()),
        version.description());
  }

  private static void dseVersionCheck(DseVersion version) {
    VersionNumber dseVersion = CCMBridge.getGlobalDSEVersion();
    if (dseVersion != null) {
      versionCheck(
          CCMBridge.getGlobalDSEVersion(),
          VersionNumber.parse(version.value()),
          version.description());
    } else {
      throw new SkipException(
          "Skipping test because not configured for DataStax Enterprise cluster.");
    }
  }

  private static void scyllaVersionCheck(ScyllaVersion annotation) {
    VersionNumber configuredVersion = CCMBridge.getGlobalScyllaVersion();
    if (configuredVersion == null) {
      throw new SkipException(
          "Skipping test because provided Scylla version is null and the test requires Scylla.");
    }
    boolean isEnterprise = String.valueOf(configuredVersion.getMajor()).matches("\\d{4}");

    if (isEnterprise) {
      if (!annotation.minEnterprise().isEmpty()) {
        VersionNumber minVersion =
            Objects.requireNonNull(VersionNumber.parse(annotation.minEnterprise()));
        if (minVersion.compareTo(configuredVersion) > 0) {
          throw new SkipException(
              String.format(
                  "Version >= %s required, but found %s. Justification: %s",
                  minVersion, configuredVersion, annotation.description()));
        }
      }
      if (!annotation.maxEnterprise().isEmpty()) {
        VersionNumber maxVersion =
            Objects.requireNonNull(VersionNumber.parse(annotation.maxEnterprise()));
        if (maxVersion.compareTo(configuredVersion) <= 0) {
          throw new SkipException(
              String.format(
                  "Version < %s required, but found %s. Justification: %s",
                  maxVersion, configuredVersion, annotation.description()));
        }
      }
    } else {
      if (!annotation.minOSS().isEmpty()) {
        VersionNumber minVersion = Objects.requireNonNull(VersionNumber.parse(annotation.minOSS()));
        if (minVersion.compareTo(configuredVersion) > 0) {
          throw new SkipException(
              String.format(
                  "Version >= %s required, but found %s. Justification: %s",
                  minVersion, configuredVersion, annotation.description()));
        }
      }
      if (!annotation.maxOSS().isEmpty()) {
        VersionNumber maxVersion = Objects.requireNonNull(VersionNumber.parse(annotation.maxOSS()));
        if (maxVersion.compareTo(configuredVersion) <= 0) {
          throw new SkipException(
              String.format(
                  "Version < %s required, but found %s. Justification: %s",
                  maxVersion, configuredVersion, annotation.description()));
        }
      }
    }
  }

  private static void versionCheck(
      VersionNumber current, VersionNumber required, String skipString) {
    if (current == null) {
      throw new SkipException("Skipping test because provided version is null");
    } else {
      if (current.compareTo(required) < 0) {
        throw new SkipException(
            String.format(
                "Version >= %s required, but found %s. Justification: %s",
                required, current, skipString));
      }
    }
  }
}
