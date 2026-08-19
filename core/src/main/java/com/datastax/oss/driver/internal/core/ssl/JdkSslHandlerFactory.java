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
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import javax.net.ssl.SSLEngine;
import net.jcip.annotations.ThreadSafe;

/** SSL handler factory used when JDK-based SSL was configured through the driver's public API. */
@ThreadSafe
public class JdkSslHandlerFactory implements SslHandlerFactory {
  private static final AttributeKey<HandlerReference> HANDLER_REFERENCE =
      AttributeKey.valueOf(JdkSslHandlerFactory.class, "sslHandler");

  private final SslEngineFactory sslEngineFactory;

  public JdkSslHandlerFactory(SslEngineFactory sslEngineFactory) {
    this.sslEngineFactory = sslEngineFactory;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
    SSLEngine engine = sslEngineFactory.newSslEngine(remoteEndpoint);
    SslHandler handler = new SslHandler(engine);
    // ChannelFactory calls this before NettyOptions.afterChannelInitialized(), so the first handler
    // recorded here is the one installed by the driver even if the hook adds more SSL handlers.
    channel.attr(HANDLER_REFERENCE).setIfAbsent(new HandlerReference(this, handler));
    return handler;
  }

  /**
   * Whether the SSL engine built for {@code channel} verifies host names, or {@code null} when it
   * cannot be determined.
   *
   * <p>This deliberately retains the exact handler returned by {@link #newSslHandler}, and reads
   * its engine lazily only while that handler remains in the channel pipeline. Engine introspection
   * is diagnostic work and belongs under the configuration reporter's fail-safe; doing it while the
   * handler is created would let a user-supplied engine that throws from {@code getSSLParameters()}
   * prevent every connection, even when reporting is disabled.
   */
  @Nullable
  public Boolean getHostnameValidationRequired(Channel channel) {
    HandlerReference reference = channel.attr(HANDLER_REFERENCE).get();
    if (reference == null
        || reference.factory != this
        || channel.pipeline().context(reference.handler) == null) {
      return null;
    }
    String endpointIdentificationAlgorithm =
        reference.handler.engine().getSSLParameters().getEndpointIdentificationAlgorithm();
    if (endpointIdentificationAlgorithm != null && !endpointIdentificationAlgorithm.isEmpty()) {
      return true;
    } else if (sslEngineFactory.getClass() == DefaultSslEngineFactory.class) {
      // The built-in configuration factory is the only path where absence of the algorithm is a
      // known disabled setting. ProgrammaticSslEngineFactory accepts an arbitrary SSLContext whose
      // trust manager could verify host names itself, and custom factories have the same ambiguity;
      // report those as unknown instead of falsely claiming that verification is off.
      return false;
    }
    return null;
  }

  @Override
  public void close() throws Exception {
    sslEngineFactory.close();
  }

  private static final class HandlerReference {
    private final JdkSslHandlerFactory factory;
    private final SslHandler handler;

    private HandlerReference(JdkSslHandlerFactory factory, SslHandler handler) {
      this.factory = factory;
      this.handler = handler;
    }
  }
}
