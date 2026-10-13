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
package com.datastax.oss.driver.internal.core.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLEngine;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;

/** SSL handler factory used when JDK-based SSL was configured through the driver's public API. */
@ThreadSafe
public class JdkSslHandlerFactory implements SslHandlerFactory {
  private final SslEngineFactory sslEngineFactory;
  private final ThreadPoolExecutor executor;

  @GuardedBy("this")
  private final Set<Preparation> pending = new HashSet<>();

  @GuardedBy("this")
  private boolean closed;

  public JdkSslHandlerFactory(SslEngineFactory sslEngineFactory) {
    this(
        sslEngineFactory,
        new ThreadPoolExecutor(
            2,
            2,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("driver-ssl-preparation-%d")
                .setDaemon(true)
                .build()));
  }

  JdkSslHandlerFactory(
      @NonNull SslEngineFactory sslEngineFactory, @NonNull ThreadPoolExecutor executor) {
    this.sslEngineFactory = sslEngineFactory;
    this.executor = executor;
  }

  /**
   * The engine factory this handler factory actually builds its engines from.
   *
   * <p>Not necessarily the one behind {@code DriverContext#getSslEngineFactory()}: a context that
   * overrides {@code buildSslHandlerFactory()} may wrap an engine factory of its own choosing, in
   * which case the configured one is never consulted on the connection path. Diagnostics that want
   * to describe the engine in force must read it from here rather than from the context.
   */
  public SslEngineFactory getSslEngineFactory() {
    return sslEngineFactory;
  }

  @NonNull
  @Override
  public synchronized CompletionStage<SslHandlerFactory> prepare(@NonNull EndPoint remoteEndpoint) {
    if (closed) {
      CompletableFuture<SslHandlerFactory> result = new CompletableFuture<>();
      result.completeExceptionally(new IllegalStateException("SSL handler factory is closed"));
      return result;
    }
    // A low-level subclass might override newSslHandler; do not bypass that extension point.
    if (getClass() != JdkSslHandlerFactory.class) {
      return SslHandlerFactory.super.prepare(remoteEndpoint);
    }
    Preparation preparation = new Preparation(remoteEndpoint);
    pending.add(preparation);
    try {
      executor.execute(preparation.task);
    } catch (RuntimeException error) {
      preparation.completeExceptionally(error);
    }
    return preparation;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
    SSLEngine engine = sslEngineFactory.newSslEngine(remoteEndpoint);
    return new SslHandler(engine);
  }

  @Override
  public void close() throws Exception {
    List<Preparation> preparations;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      preparations = new ArrayList<>(pending);
    }
    // DNS lookups are not necessarily interruptible. Finish callers promptly without waiting for
    // workers; any engine produced after cancellation is released by Preparation.
    for (Preparation preparation : preparations) {
      preparation.completeExceptionally(new IllegalStateException("SSL handler factory is closed"));
    }
    executor.shutdownNow();
    sslEngineFactory.close();
  }

  @ThreadSafe
  private final class Preparation extends CompletableFuture<SslHandlerFactory> {
    private final FutureTask<Void> task;

    private Preparation(@NonNull EndPoint remoteEndpoint) {
      task =
          new FutureTask<>(
              () -> {
                if (!isDone()) {
                  try {
                    PreparedFactory prepared =
                        new PreparedFactory(sslEngineFactory.newSslEngine(remoteEndpoint));
                    if (!complete(prepared)) {
                      prepared.close();
                    }
                  } catch (Throwable error) {
                    completeExceptionally(error);
                  }
                }
                return null;
              });
      whenComplete(
          (prepared, error) -> {
            synchronized (JdkSslHandlerFactory.this) {
              pending.remove(this);
            }
            if (error != null) {
              task.cancel(true);
              // FutureTask cancellation alone leaves the task in ThreadPoolExecutor's queue.
              executor.remove(task);
            }
          });
    }
  }

  @ThreadSafe
  private static final class PreparedFactory implements SslHandlerFactory {
    private final AtomicReference<SSLEngine> engine;

    private PreparedFactory(@NonNull SSLEngine engine) {
      this.engine = new AtomicReference<>(engine);
    }

    @Override
    public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
      SSLEngine prepared = engine.getAndSet(null);
      if (prepared == null) {
        throw new IllegalStateException("Prepared SSL engine has already been used or closed");
      }
      try {
        return new SslHandler(prepared);
      } catch (Throwable error) {
        release(prepared);
        throw error;
      }
    }

    @Override
    public void close() {
      SSLEngine unused = engine.getAndSet(null);
      if (unused != null) {
        release(unused);
      }
    }

    private static void release(@NonNull SSLEngine engine) {
      try {
        engine.closeOutbound();
      } finally {
        ReferenceCountUtil.safeRelease(engine);
      }
    }
  }
}
