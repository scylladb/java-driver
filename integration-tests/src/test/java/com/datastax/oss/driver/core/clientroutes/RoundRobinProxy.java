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

/*
 * Copyright (C) 2025 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.clientroutes;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TCP proxy that round-robins incoming connections across multiple backend targets. Each new
 * connection is forwarded to the next backend in the list.
 */
public class RoundRobinProxy implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinProxy.class);
  private static final int BUFFER_SIZE = 8192;

  private final ServerSocket serverSocket;
  private final CopyOnWriteArrayList<InetSocketAddress> targets;
  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final CopyOnWriteArrayList<Socket> activeSockets = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Thread> pipeThreads = new CopyOnWriteArrayList<>();
  private final Thread acceptThread;

  public RoundRobinProxy(String bindAddress, int listenPort, List<InetSocketAddress> targets)
      throws IOException {
    if (targets.isEmpty()) {
      throw new IllegalArgumentException("At least one target required");
    }
    this.targets = new CopyOnWriteArrayList<>(targets);
    this.serverSocket = new ServerSocket();
    this.serverSocket.setReuseAddress(true);
    this.serverSocket.bind(new InetSocketAddress(bindAddress, listenPort));
    this.acceptThread =
        new Thread(this::acceptLoop, "rr-proxy-accept-" + serverSocket.getLocalPort());
    this.acceptThread.setDaemon(true);
    this.acceptThread.start();
    LOG.debug(
        "RoundRobinProxy listening on {} -> {} targets", serverSocket.getLocalPort(), targets);
  }

  public int getLocalPort() {
    return serverSocket.getLocalPort();
  }

  /** Adds a target to the round-robin pool. */
  public void addTarget(InetSocketAddress target) {
    targets.add(target);
    LOG.debug("RoundRobinProxy on port {} added target: {}", serverSocket.getLocalPort(), target);
  }

  /** Removes a target from the round-robin pool. */
  public void removeTarget(InetSocketAddress target) {
    targets.remove(target);
    LOG.debug("RoundRobinProxy on port {} removed target: {}", serverSocket.getLocalPort(), target);
  }

  private void acceptLoop() {
    while (!closed.get()) {
      try {
        Socket client = serverSocket.accept();
        if (closed.get()) {
          client.close();
          break;
        }
        activeSockets.add(client);

        List<InetSocketAddress> snapshot = new ArrayList<>(targets);
        if (snapshot.isEmpty()) {
          closeQuietly(client);
          LOG.warn("RoundRobinProxy has no targets, rejecting connection");
          continue;
        }
        InetSocketAddress target =
            snapshot.get(Math.floorMod(counter.getAndIncrement(), snapshot.size()));
        Socket remote = new Socket();
        activeSockets.add(remote);
        try {
          remote.connect(target, 5000);
        } catch (IOException e) {
          closeQuietly(client);
          closeQuietly(remote);
          if (!closed.get()) {
            LOG.warn("RoundRobinProxy connect to target failed", e);
          }
          continue;
        }

        Thread c2r =
            new Thread(
                () -> pipe(client, remote),
                "rr-proxy-c2r-" + serverSocket.getLocalPort() + "-" + client.getPort());
        Thread r2c =
            new Thread(
                () -> pipe(remote, client),
                "rr-proxy-r2c-" + serverSocket.getLocalPort() + "-" + client.getPort());
        c2r.setDaemon(true);
        r2c.setDaemon(true);
        pipeThreads.add(c2r);
        pipeThreads.add(r2c);
        c2r.start();
        r2c.start();
      } catch (SocketException e) {
        if (!closed.get()) {
          LOG.warn("RoundRobinProxy accept error", e);
        }
      } catch (IOException e) {
        if (!closed.get()) {
          LOG.warn("RoundRobinProxy accept error", e);
        }
      }
    }
  }

  private void pipe(Socket from, Socket to) {
    byte[] buf = new byte[BUFFER_SIZE];
    try {
      InputStream in = from.getInputStream();
      OutputStream out = to.getOutputStream();
      int n;
      while ((n = in.read(buf)) >= 0) {
        out.write(buf, 0, n);
        out.flush();
      }
    } catch (IOException e) {
      // expected when sockets close
    } finally {
      closeQuietly(from);
      closeQuietly(to);
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      closeQuietly(serverSocket);
      for (Socket s : activeSockets) {
        closeQuietly(s);
      }
      activeSockets.clear();
      try {
        acceptThread.join(5000);
        if (acceptThread.isAlive()) {
          LOG.warn("RoundRobinProxy accept thread still alive after join timeout");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      for (Thread t : pipeThreads) {
        try {
          t.join(5000);
          if (t.isAlive()) {
            LOG.warn("RoundRobinProxy pipe thread {} still alive after join timeout", t.getName());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      pipeThreads.clear();
    }
  }

  private static void closeQuietly(Closeable c) {
    try {
      if (c != null) c.close();
    } catch (IOException ignored) {
    }
  }
}
