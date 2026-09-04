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
package com.datastax.oss.driver.internal.core.addresstranslation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import javax.naming.NamingException;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.InitialDirContext;
import org.junit.Test;

public class Ec2MultiRegionAddressTranslatorTest {

  @Test
  public void should_return_same_address_when_no_entry_found() throws Exception {
    InitialDirContext mock = mock(InitialDirContext.class);
    when(mock.getAttributes(anyString(), any(String[].class))).thenReturn(new BasicAttributes());
    Ec2MultiRegionAddressTranslator translator = new Ec2MultiRegionAddressTranslator(mock);

    InetSocketAddress address = new InetSocketAddress("192.0.2.5", 9042);
    assertThat(translator.translate(address)).isEqualTo(address);
  }

  @Test
  public void should_return_same_address_when_exception_encountered() throws Exception {
    InitialDirContext mock = mock(InitialDirContext.class);
    when(mock.getAttributes(anyString(), any(String[].class)))
        .thenThrow(new NamingException("Problem resolving address (not really)."));
    Ec2MultiRegionAddressTranslator translator = new Ec2MultiRegionAddressTranslator(mock);

    InetSocketAddress address = new InetSocketAddress("192.0.2.5", 9042);
    assertThat(translator.translate(address)).isEqualTo(address);
  }

  @Test
  public void should_return_same_address_when_the_domain_name_does_not_resolve() throws Exception {
    // The third way this translator can fail, and the one the deferred forward lookup nearly took
    // away: the PTR record answers, but the name it gives has no A record -- private DNS switched
    // off on the VPC, split-horizon DNS that serves the reverse zone only, a stale PTR after an
    // instance replacement. Handing that name over unchecked would strand the node for good, since
    // the connect layer has nothing to fall back to and every refresh re-derives the same name.
    // So the forward lookup still runs here, and its failure means the node keeps the raw
    // broadcast address it was already reachable on.
    assumeThat(new InetSocketAddress("node1.eu-west-1.example.com", 9042).isUnresolved())
        .as("requires a host whose resolver does not answer for unregistered names")
        .isTrue();

    InitialDirContext mock = mock(InitialDirContext.class);
    when(mock.getAttributes("5.2.0.192.in-addr.arpa", new String[] {"PTR"}))
        .thenReturn(new BasicAttributes("PTR", "node1.eu-west-1.example.com"));
    Ec2MultiRegionAddressTranslator translator = new Ec2MultiRegionAddressTranslator(mock);

    InetSocketAddress address = new InetSocketAddress("192.0.2.5", 9042);
    assertThat(translator.translate(address)).isEqualTo(address);
  }

  @Test
  public void should_not_resolve_a_domain_name_that_would_resolve() {
    // The "match found" case, and it has to use a name that really resolves: an unresolvable one
    // is answered with the original address (see above), and even without that, `new
    // InetSocketAddress(String, int)` leaves it unresolved too, so the two spellings would compare
    // equal on host string and port and this could not tell them apart.
    assumeThat(new InetSocketAddress("localhost", 9042).isUnresolved())
        .as("requires a host where localhost resolves; where it does not, both spellings agree")
        .isFalse();

    InitialDirContext mock = mock(InitialDirContext.class);
    Ec2MultiRegionAddressTranslator translator;
    try {
      when(mock.getAttributes("5.2.0.192.in-addr.arpa", new String[] {"PTR"}))
          .thenReturn(new BasicAttributes("PTR", "localhost"));
      translator = new Ec2MultiRegionAddressTranslator(mock);
    } catch (NamingException impossible) {
      throw new AssertionError(impossible);
    }

    // The forward lookup belongs to ChannelFactory, which expands the name to every address it
    // maps to. Doing it here keeps one and the rest are never tried, because the resolver reports
    // an already-resolved address as nothing to do.
    InetSocketAddress translated = translator.translate(new InetSocketAddress("192.0.2.5", 9042));
    assertThat(translated.isUnresolved()).isTrue();
    assertThat(translated.getHostString()).isEqualTo("localhost");
    assertThat(translated.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_close_context_when_closed() throws Exception {
    InitialDirContext mock = mock(InitialDirContext.class);
    Ec2MultiRegionAddressTranslator translator = new Ec2MultiRegionAddressTranslator(mock);

    // ensure close has not been called to this point.
    verify(mock, times(0)).close();
    translator.close();
    // ensure close is closed.
    verify(mock).close();
  }

  @Test
  public void should_build_reversed_domain_name_for_ip_v4() throws Exception {
    InetAddress address = InetAddress.getByName("192.0.2.5");
    assertThat(Ec2MultiRegionAddressTranslator.reverse(address))
        .isEqualTo("5.2.0.192.in-addr.arpa");
  }

  @Test
  public void should_build_reversed_domain_name_for_ip_v6() throws Exception {
    InetAddress address = InetAddress.getByName("2001:db8::567:89ab");
    assertThat(Ec2MultiRegionAddressTranslator.reverse(address))
        .isEqualTo("b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.ip6.arpa");
  }
}
