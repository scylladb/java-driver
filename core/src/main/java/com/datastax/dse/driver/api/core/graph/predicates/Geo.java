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

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/** @deprecated DSE Graph is not supported starting with driver 4.19.2.2. */
@SuppressWarnings("DoNotCallSuggester")
@Deprecated
public interface Geo {

  enum Unit {
    MILES,
    KILOMETERS,
    METERS,
    DEGREES;

    /** Convert distance to degrees (used internally only). */
    public double toDegrees(double distance) {
      throw GraphSupportRemoved.exception();
    }
  }

  /**
   * Finds whether an entity is inside the given circular area using a geo coordinate system.
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> inside(Point center, double radius, Unit units) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Finds whether an entity is inside the given circular area using a cartesian coordinate system.
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> inside(Point center, double radius) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Finds whether an entity is inside the given polygon.
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> inside(Polygon polygon) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Creates a point from the given coordinates.
   *
   * <p>This is just a shortcut to {@link Point#fromCoordinates(double, double)}. It is duplicated
   * here so that {@code Geo} can be used as a single entry point in Gremlin-groovy scripts.
   */
  @NonNull
  static Point point(double x, double y) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Creates a line string from the given (at least 2) points.
   *
   * <p>This is just a shortcut to {@link LineString#fromPoints(Point, Point, Point...)}. It is
   * duplicated here so that {@code Geo} can be used as a single entry point in Gremlin-groovy
   * scripts.
   */
  @NonNull
  static LineString lineString(
      @NonNull Point point1, @NonNull Point point2, @NonNull Point... otherPoints) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Creates a line string from the coordinates of its points.
   *
   * <p>This is provided for backward compatibility with previous DSE versions. We recommend {@link
   * #lineString(Point, Point, Point...)} instead.
   */
  @NonNull
  static LineString lineString(double... coordinates) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Creates a polygon from the given (at least 3) points.
   *
   * <p>This is just a shortcut to {@link Polygon#fromPoints(Point, Point, Point, Point...)}. It is
   * duplicated here so that {@code Geo} can be used as a single entry point in Gremlin-groovy
   * scripts.
   */
  @NonNull
  static Polygon polygon(
      @NonNull Point p1, @NonNull Point p2, @NonNull Point p3, @NonNull Point... otherPoints) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Creates a polygon from the coordinates of its points.
   *
   * <p>This is provided for backward compatibility with previous DSE versions. We recommend {@link
   * #polygon(Point, Point, Point, Point...)} instead.
   */
  @NonNull
  static Polygon polygon(double... coordinates) {
    throw GraphSupportRemoved.exception();
  }
}
