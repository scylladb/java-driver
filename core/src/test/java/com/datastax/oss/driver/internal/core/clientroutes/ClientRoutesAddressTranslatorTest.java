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
package com.datastax.oss.driver.internal.core.clientroutes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientRoutesAddressTranslatorTest {

  @Mock private InternalDriverContext context;
  @Mock private ClientRoutesHandler handler;
  @Mock private SslEngineFactory sslEngineFactory;

  private ClientRoutesAddressTranslator translator;

  private static final InetSocketAddress ORIGINAL = new InetSocketAddress("10.0.0.1", 9042);
  private static final InetSocketAddress TRANSLATED = new InetSocketAddress("192.168.1.1", 19042);

  @Before
  public void setup() {
    when(context.getSessionName()).thenReturn("test-session");
    when(context.getClientRoutesHandler()).thenReturn(handler);
    when(context.getSslEngineFactory()).thenReturn(Optional.empty());
    translator = new ClientRoutesAddressTranslator(context);
  }

  @Test
  public void should_bypass_translation_for_null_host_id() {
    InetSocketAddress result = translator.translate(ORIGINAL, null, "dc1", "rack1");

    assertThat(result).isEqualTo(ORIGINAL);
    verifyZeroInteractions(handler);
  }

  @Test
  public void should_bypass_translation_via_legacy_translate_method() {
    // The no-arg translate is used for contact points
    InetSocketAddress result = translator.translate(ORIGINAL);

    assertThat(result).isEqualTo(ORIGINAL);
    verifyZeroInteractions(handler);
  }

  @Test
  public void should_translate_known_host_id() {
    UUID hostId = UUID.randomUUID();
    when(handler.translate(eq(hostId), anyBoolean())).thenReturn(TRANSLATED);

    InetSocketAddress result = translator.translate(ORIGINAL, hostId, "dc1", "rack1");

    assertThat(result).isEqualTo(TRANSLATED);
    verify(handler).translate(eq(hostId), anyBoolean());
  }

  @Test
  public void should_fallback_to_original_when_handler_returns_null() {
    UUID hostId = UUID.randomUUID();
    when(handler.translate(eq(hostId), anyBoolean())).thenReturn(null);

    InetSocketAddress result = translator.translate(ORIGINAL, hostId, "dc1", "rack1");

    assertThat(result).isEqualTo(ORIGINAL);
  }

  @Test
  public void should_pass_ssl_flag_based_on_ssl_engine_factory() {
    UUID hostId = UUID.randomUUID();
    // Enable SSL
    when(context.getSslEngineFactory()).thenReturn(Optional.of(sslEngineFactory));
    when(handler.translate(eq(hostId), eq(true))).thenReturn(TRANSLATED);
    // Recreate translator so it picks up new SSL setting
    translator = new ClientRoutesAddressTranslator(context);

    InetSocketAddress result = translator.translate(ORIGINAL, hostId, "dc1", "rack1");

    assertThat(result).isEqualTo(TRANSLATED);
    verify(handler).translate(hostId, true);
  }

  @Test
  public void should_handle_null_handler_gracefully() {
    when(context.getClientRoutesHandler()).thenReturn(null);
    // Recreate translator so it uses null handler
    translator = new ClientRoutesAddressTranslator(context);
    UUID hostId = UUID.randomUUID();

    // Should return original address, not throw
    InetSocketAddress result = translator.translate(ORIGINAL, hostId, "dc1", "rack1");

    assertThat(result).isEqualTo(ORIGINAL);
  }

  @Test
  public void close_does_not_close_handler() {
    // Handler lifecycle is managed by context; translator.close() must be a no-op
    translator.close();
    // handler was never closed — no interactions
    verifyZeroInteractions(handler);
  }
}
