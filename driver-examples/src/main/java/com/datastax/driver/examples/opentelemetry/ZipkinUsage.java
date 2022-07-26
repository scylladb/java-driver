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
package com.datastax.driver.examples.opentelemetry;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.tracing.TracingInfoFactory;
import com.datastax.driver.opentelemetry.OpenTelemetryTracingInfoFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

/**
 * Creates a keyspace and tables, and loads some data into them. Sends OpenTelemetry tracing data to
 * Zipkin tracing backend
 *
 * <p>Preconditions: - a Scylla cluster is running and accessible through the contacts points
 * identified by CONTACT_POINTS and PORT and Zipkin backend is running and accessible through the
 * contacts points identified by ZIPKIN_CONTACT_POINT and ZIPKIN_PORT.
 *
 * <p>Side effects: - creates a new keyspace "simplex" in the cluster. If a keyspace with this name
 * already exists, it will be reused; - creates two tables "simplex.songs" and "simplex.playlists".
 * If they exist already, they will be reused; - inserts a row in each table.
 */
public class ZipkinUsage {
  private static final String CONTACT_POINT = "127.0.0.1";
  private static final int PORT = 9042;

  private static final String ZIPKIN_CONTACT_POINT = "127.0.0.1";
  private static final int ZIPKIN_PORT = 9411;

  private Cluster cluster;
  private Session session;

  private Tracer tracer;

  public static void main(String[] args) {
    // Workaround for setting ContextStorage to ThreadLocalContextStorage.
    System.setProperty("io.opentelemetry.context.contextStorageProvider", "default");

    ZipkinUsage client = new ZipkinUsage();

    try {
      client.connect();
      client.createSchema();
      client.loadData();
      client.querySchema();
      System.out.println(
          "All requests have been completed. Now you can visit Zipkin at "
              + "http://"
              + ZIPKIN_CONTACT_POINT
              + ":"
              + ZIPKIN_PORT
              + " and examine the produced trace.");
    } finally {
      client.close();
    }
  }

  /** Initiates a connection to the cluster. */
  public void connect() {
    cluster = Cluster.builder().addContactPoints(CONTACT_POINT).withPort(PORT).build();

    System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());

    OpenTelemetry openTelemetry =
        OpenTelemetryConfiguration.initializeForZipkin(ZIPKIN_CONTACT_POINT, ZIPKIN_PORT);
    tracer = openTelemetry.getTracerProvider().get("this");
    TracingInfoFactory tracingInfoFactory = new OpenTelemetryTracingInfoFactory(tracer);
    cluster.setTracingInfoFactory(tracingInfoFactory);

    session = cluster.connect();
  }

  /** Creates the schema (keyspace) and tables for this example. */
  public void createSchema() {
    session.execute("DROP KEYSPACE IF EXISTS simplex;");
    Span parentSpan = tracer.spanBuilder("create schema").startSpan();
    try (Scope parentScope = parentSpan.makeCurrent()) {
      {
        Span span = tracer.spanBuilder("create simplex").startSpan();
        try (Scope scope = span.makeCurrent()) {
          session.execute(
              "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
                  + "= {'class':'SimpleStrategy', 'replication_factor':1};");

        } finally {
          span.end();
        }
      }
      {
        Span span = tracer.spanBuilder("create simplex.songs").startSpan();
        try (Scope scope = span.makeCurrent()) {
          session.executeAsync(
              "CREATE TABLE IF NOT EXISTS simplex.songs ("
                  + "id uuid,"
                  + "title text,"
                  + "album text,"
                  + "artist text,"
                  + "tags set<text>,"
                  + "data blob,"
                  + "PRIMARY KEY ((title, artist), album)"
                  + ");");
        } finally {
          span.end();
        }
      }
      {
        Span span = tracer.spanBuilder("create simplex.playlists").startSpan();
        try (Scope scope = span.makeCurrent()) {
          session.execute(
              "CREATE TABLE IF NOT EXISTS simplex.playlists ("
                  + "id uuid,"
                  + "title text,"
                  + "album text, "
                  + "artist text,"
                  + "song_id uuid,"
                  + "PRIMARY KEY (id, title, album, artist)"
                  + ");");

        } finally {
          span.end();
        }
      }
    } finally {
      parentSpan.end();
    }
  }

  /** Inserts data into the tables. */
  public void loadData() {
    Span parentSpan = tracer.spanBuilder("load data").startSpan();
    try (Scope parentScope = parentSpan.makeCurrent()) {

      Span prepareSpan = tracer.spanBuilder("prepare").startSpan();
      PreparedStatement ps;
      try (Scope prepareScope = prepareSpan.makeCurrent()) {
        ps =
            session.prepare(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) "
                    + "VALUES ("
                    + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                    + "?,"
                    + "?,"
                    + "?,"
                    + "{'jazz', '2013'})"
                    + ";");
      } finally {
        prepareSpan.end();
      }

      Span bindSpan = tracer.spanBuilder("bind").startSpan();
      BoundStatement bound;
      try (Scope bindScope = bindSpan.makeCurrent()) {
        bound = ps.bind("La Petite Tonkinoise", "Bye Bye Blackbird", "Joséphine Baker");
      } finally {
        bindSpan.end();
      }

      Span span = tracer.spanBuilder("insert simplex.songs").startSpan();
      try (Scope scope = span.makeCurrent()) {
        session.execute(bound);
      } finally {
        span.end();
      }

    } finally {
      parentSpan.end();
    }
  }

  public void querySchema() {
    Span parentSpan = tracer.spanBuilder("query schema").startSpan();
    try (Scope parentScope = parentSpan.makeCurrent()) {

      Span prepareSpan = tracer.spanBuilder("prepare").startSpan();
      PreparedStatement ps;
      try (Scope prepareScope = prepareSpan.makeCurrent()) {
        ps = session.prepare("SELECT * FROM simplex.songs WHERE artist = ? AND title = ?;");
      } finally {
        prepareSpan.end();
      }

      Span bindSpan = tracer.spanBuilder("bind").startSpan();
      BoundStatement bound;
      try (Scope bindScope = bindSpan.makeCurrent()) {
        bound = ps.bind("Joséphine Baker", "La Petite Tonkinoise");
      } finally {
        bindSpan.end();
      }

      Span span = tracer.spanBuilder("query simplex.songs").startSpan();
      try (Scope scope = span.makeCurrent()) {
        session.execute(bound);
      } finally {
        span.end();
      }

    } finally {
      parentSpan.end();
    }
  }

  /** Closes the session and the cluster. */
  public void close() {
    session.close();
    cluster.close();
  }
}
