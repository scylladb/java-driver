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
package com.datastax.dse.driver.api.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dse.driver.api.core.auth.DseGssApiAuthProviderBase.GssApiAuthenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.channel.LocalEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.security.sasl.SaslException;
import org.junit.Test;

public class DseGssApiAuthProviderBaseTest {

  @Test
  public void should_not_let_resolution_change_the_server_name() throws Exception {
    // The principal must name the same host whether or not the contact point was resolved: with
    // advanced.resolve-contact-points = false (the default) getAddress() is null, and the old code
    // dereferenced it. "::1" keeps this independent of the local reverse zone, since a canonical
    // name is either a PTR name or, when that lookup fails, the expanded literal
    // (InetAddress:686-693), and never the compressed form the host string carries.
    EndPoint unresolved = new DefaultEndPoint(InetSocketAddress.createUnresolved("::1", 9042));
    EndPoint resolved =
        new DefaultEndPoint(new InetSocketAddress(InetAddress.getByName("::1"), 9042));

    assertThat(GssApiAuthenticator.serverName(unresolved))
        .isEqualTo(GssApiAuthenticator.serverName(resolved))
        .isNotEqualTo("::1");
  }

  @Test
  public void should_canonicalize_an_alias_on_a_resolved_endpoint() throws Exception {
    // The alias case the javadoc is about: a name that is not the host's canonical name must not
    // reach the KDC verbatim. getByAddress attaches the name without a lookup, so the host string
    // is the alias while the canonical name is whatever 127.0.0.1 canonicalizes to, which is never
    // the alias. Independent of the reverse zone for the same reason as the test above.
    InetAddress alias = InetAddress.getByAddress("alias.invalid", new byte[] {127, 0, 0, 1});
    EndPoint endPoint = new DefaultEndPoint(new InetSocketAddress(alias, 9042));

    assertThat(GssApiAuthenticator.serverName(endPoint))
        .isEqualTo(alias.getCanonicalHostName())
        .isNotEqualTo("alias.invalid");
  }

  @Test
  public void should_fall_back_to_host_string_when_it_cannot_be_canonicalized() throws Exception {
    // RFC 6761 reserves ".invalid" as guaranteed NXDOMAIN, so the fallback is the only possible
    // answer and this test needs no working resolver.
    EndPoint endPoint = new DefaultEndPoint(InetSocketAddress.createUnresolved("db.invalid", 9042));

    assertThat(GssApiAuthenticator.serverName(endPoint)).isEqualTo("db.invalid");
  }

  @Test
  public void should_reject_an_endpoint_that_does_not_resolve_to_an_ip() {
    // EndPoint.resolve() is declared to return SocketAddress, so a custom implementation may hand
    // back something that is not an InetSocketAddress; the cast used to throw a ClassCastException
    // that escaped the authenticator's catch clause.
    assertThatThrownBy(() -> GssApiAuthenticator.serverName(new LocalEndPoint("gssapi-test")))
        .isInstanceOf(SaslException.class)
        .hasMessageContaining("does not resolve to an IP address");
  }
}
