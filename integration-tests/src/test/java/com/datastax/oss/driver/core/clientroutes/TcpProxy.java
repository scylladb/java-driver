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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple TCP proxy that forwards connections from a local port to a remote target. Each accepted
 * connection spawns two threads to pipe data in both directions.
 */
public class TcpProxy implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TcpProxy.class);
  private static final int BUFFER_SIZE = 8192;

  private final ServerSocket serverSocket;
  private final InetSocketAddress target;
  private final boolean proxyProtocol;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final CopyOnWriteArrayList<Socket> activeSockets = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Thread> pipeThreads = new CopyOnWriteArrayList<>();
  private final Thread acceptThread;

  /**
   * Creates and starts a TCP proxy.
   *
   * @param bindAddress the local address to bind on
   * @param listenPort the local port to listen on (0 for any available port)
   * @param target the remote address to forward connections to
   */
  public TcpProxy(String bindAddress, int listenPort, InetSocketAddress target) throws IOException {
    this(bindAddress, listenPort, target, false);
  }

  /**
   * Creates and starts a TCP proxy with optional Proxy Protocol v2 support.
   *
   * @param bindAddress the local address to bind on
   * @param listenPort the local port to listen on (0 for any available port)
   * @param target the remote address to forward connections to
   * @param proxyProtocol when {@code true}, a PP2 binary header is prepended to each forwarded
   *     connection carrying the original client IP and source port
   */
  public TcpProxy(
      String bindAddress, int listenPort, InetSocketAddress target, boolean proxyProtocol)
      throws IOException {
    this.target = target;
    this.proxyProtocol = proxyProtocol;
    this.serverSocket = new ServerSocket();
    this.serverSocket.setReuseAddress(true);
    this.serverSocket.bind(new InetSocketAddress(bindAddress, listenPort));
    this.acceptThread =
        new Thread(this::acceptLoop, "tcp-proxy-accept-" + serverSocket.getLocalPort());
    this.acceptThread.setDaemon(true);
    this.acceptThread.start();
    LOG.debug(
        "TcpProxy listening on {} -> {} (proxyProtocol={})",
        serverSocket.getLocalPort(),
        target,
        proxyProtocol);
  }

  /** Returns the local port this proxy is listening on. */
  public int getLocalPort() {
    return serverSocket.getLocalPort();
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
        Socket remote = new Socket();
        activeSockets.add(remote);
        try {
          remote.connect(target, 5000);
        } catch (IOException e) {
          closeQuietly(client);
          closeQuietly(remote);
          if (!closed.get()) {
            LOG.warn("TcpProxy connect to target failed", e);
          }
          continue;
        }

        if (proxyProtocol) {
          try {
            sendPp2Header(remote.getOutputStream(), client);
          } catch (IOException e) {
            closeQuietly(client);
            closeQuietly(remote);
            if (!closed.get()) {
              LOG.warn("TcpProxy failed to write PP2 header", e);
            }
            continue;
          }
        }

        Thread c2r =
            new Thread(
                () -> pipe(client, remote),
                "tcp-proxy-c2r-" + serverSocket.getLocalPort() + "-" + client.getPort());
        Thread r2c =
            new Thread(
                () -> pipe(remote, client),
                "tcp-proxy-r2c-" + serverSocket.getLocalPort() + "-" + client.getPort());
        c2r.setDaemon(true);
        r2c.setDaemon(true);
        pipeThreads.add(c2r);
        pipeThreads.add(r2c);
        c2r.start();
        r2c.start();
      } catch (SocketException e) {
        if (!closed.get()) {
          LOG.warn("TcpProxy accept error", e);
        }
      } catch (IOException e) {
        if (!closed.get()) {
          LOG.warn("TcpProxy accept error", e);
        }
      }
    }
  }

  /**
   * Writes a Proxy Protocol v2 (PP2) binary header to {@code out} encoding the original client
   * source address and port. ScyllaDB reads this header to determine the client's original source
   * port and uses it for shard-aware routing.
   *
   * <p>PP2 TCP4 header format (28 bytes total):
   *
   * <ul>
   *   <li>12 bytes: fixed signature
   *   <li>1 byte: version (0x2) | command PROXY (0x1) = 0x21
   *   <li>1 byte: family AF_INET (0x1) | protocol STREAM (0x1) = 0x11
   *   <li>2 bytes: address block length = 12 (big-endian)
   *   <li>4 bytes: source IPv4 (client's address)
   *   <li>4 bytes: destination IPv4 (this proxy's local address)
   *   <li>2 bytes: source port (client's port, big-endian)
   *   <li>2 bytes: destination port (this proxy's local port, big-endian)
   * </ul>
   */
  private void sendPp2Header(OutputStream out, Socket client) throws IOException {
    byte[] src = client.getInetAddress().getAddress(); // client source IP (4 bytes)
    byte[] dst = serverSocket.getInetAddress().getAddress(); // NLB bind address (4 bytes)
    int srcPort = client.getPort(); // client source port carries shard hint
    int dstPort = serverSocket.getLocalPort(); // NLB listening port

    byte[] header = new byte[28];
    // PP2 signature
    header[0] = 0x0D;
    header[1] = 0x0A;
    header[2] = 0x0D;
    header[3] = 0x0A;
    header[4] = 0x00;
    header[5] = 0x0D;
    header[6] = 0x0A;
    header[7] = 0x51;
    header[8] = 0x55;
    header[9] = 0x49;
    header[10] = 0x54;
    header[11] = 0x0A;
    header[12] = 0x21; // version 2, PROXY command
    header[13] = 0x11; // AF_INET, STREAM
    header[14] = 0x00;
    header[15] = 12; // address block length = 12
    System.arraycopy(src, 0, header, 16, 4);
    System.arraycopy(dst, 0, header, 20, 4);
    header[24] = (byte) (srcPort >> 8);
    header[25] = (byte) (srcPort & 0xFF);
    header[26] = (byte) (dstPort >> 8);
    header[27] = (byte) (dstPort & 0xFF);
    out.write(header);
    out.flush();
    LOG.debug(
        "PP2 header sent: src={}:{} dst={}:{}",
        client.getInetAddress().getHostAddress(),
        srcPort,
        serverSocket.getInetAddress().getHostAddress(),
        dstPort);
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
          LOG.warn("TcpProxy accept thread still alive after join timeout");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      for (Thread t : pipeThreads) {
        try {
          t.join(5000);
          if (t.isAlive()) {
            LOG.warn("TcpProxy pipe thread {} still alive after join timeout", t.getName());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      pipeThreads.clear();
      LOG.debug("TcpProxy on port {} closed", serverSocket.getLocalPort());
    }
  }

  private static void closeQuietly(Closeable c) {
    try {
      if (c != null) c.close();
    } catch (IOException ignored) {
    }
  }
}
