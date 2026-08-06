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

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AddressTranslator} implementation for a multi-region EC2 deployment <b>where clients are
 * also deployed in EC2</b>.
 *
 * <p>Its distinctive feature is that it translates addresses according to the location of the
 * Cassandra host:
 *
 * <ul>
 *   <li>addresses in different EC2 regions (than the client) are unchanged;
 *   <li>addresses in the same EC2 region are <b>translated to private IPs</b>.
 * </ul>
 *
 * This optimizes network costs, because Amazon charges more for communication over public IPs.
 *
 * <p>Implementation note: this class performs a reverse DNS lookup of the origin address, to find
 * the domain name of the target instance. Then it performs a forward DNS lookup of the domain name;
 * the EC2 DNS does the private/public switch automatically based on location.
 */
@SuppressWarnings("BanJNDI")
public class Ec2MultiRegionAddressTranslator implements AddressTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(Ec2MultiRegionAddressTranslator.class);

  private final DirContext ctx;
  private final String logPrefix;

  public Ec2MultiRegionAddressTranslator(
      @SuppressWarnings("unused") @NonNull DriverContext context) {
    this.logPrefix = context.getSessionName();
    @SuppressWarnings("JdkObsolete")
    Hashtable<Object, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
    try {
      ctx = new InitialDirContext(env);
    } catch (NamingException e) {
      throw new RuntimeException("Could not create translator", e);
    }
  }

  @VisibleForTesting
  Ec2MultiRegionAddressTranslator(@NonNull DirContext ctx) {
    this.logPrefix = "test";
    this.ctx = ctx;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The domain name the PTR record gives is returned {@linkplain InetSocketAddress#isUnresolved
   * () unresolved}, so that {@link com.datastax.oss.driver.internal.core.channel.ChannelFactory}
   * expands it per connect and can try every address it maps to. Only the <i>forward</i> lookup
   * moves; the reverse one below still happens here, because it is the whole point of this class.
   *
   * <p>Resolving forward here would keep whichever address came back first and no other would ever
   * be tried, since the resolver reports an already-resolved address as nothing to do and the
   * expansion is skipped. Same defect, and same fix, as {@link FixedHostNameAddressTranslator} --
   * the two get there differently ({@code InetAddress.getByName} rather than {@code new
   * InetSocketAddress(String, int)}) but arrive at the same resolved address.
   *
   * <p>The forward lookup is still <i>performed</i> here, and its result discarded: it is what
   * tells this method that the name it is about to hand over is usable, while it still holds the
   * address to fall back on if it is not. A name that does not resolve is answered with the
   * original {@code socketAddress}, exactly as it was before the lookup moved.
   */
  @NonNull
  @Override
  public InetSocketAddress translate(@NonNull InetSocketAddress socketAddress) {
    InetAddress address = socketAddress.getAddress();
    try {
      // InetAddress#getHostName() is supposed to perform a reverse DNS lookup, but for some reason
      // it doesn't work within the same EC2 region (it returns the IP address itself).
      // We use an alternate implementation:
      String domainName = lookupPtrRecord(reverse(address));
      if (domainName == null) {
        LOG.warn("[{}] Found no domain name for {}, returning it as-is", logPrefix, address);
        return socketAddress;
      }

      // Resolved and thrown away, on purpose. What moves into the connection attempt is the
      // *choice* of address -- so that every A-record gets a turn -- not the question of whether
      // the name resolves at all, and that question is this method's to answer: it is the one
      // place that still holds the working address the PTR record was found from. Returning an
      // unresolvable name unchecked strands the node for good, since the connect layer has nothing
      // to fall back to and every refresh re-derives the same name from the same PTR record. A PTR
      // whose target has no A record is not exotic -- private DNS switched off on the VPC, or
      // split-horizon DNS that answers the reverse zone but not the forward one -- and before the
      // lookup moved, the UnknownHostException it throws is what reached the catch below and handed
      // back the node's raw broadcast address.
      //
      // Exactly that question and no more. A PTR record that is merely *stale* -- naming a host
      // that still resolves, because it is the instance that replaced this one -- passes, since the
      // addresses come back only to be counted and are never compared against the one this node
      // answered on. And the question is answered by the JVM's resolver, while the connect goes
      // through whichever AddressResolverGroup NettyOptions installed: a deployment that points
      // Netty at a private zone the JVM cannot see, or hands resolution to a pipeline handler
      // entirely, would have a name rejected here that the connect could have used. Both are
      // tracked in https://github.com/scylladb/java-driver/issues/1010, and neither is fixable from
      // here -- this method is synchronous and knows nothing about Netty.
      //
      // Cheap where it sits: the JVM caches forward lookups (networkaddress.cache.ttl), and this
      // method already pays for an uncached JNDI reverse lookup on the same call.
      InetAddress[] forward = InetAddress.getAllByName(domainName);
      LOG.debug(
          "[{}] Resolved {} to {}, which maps to {} address(es)",
          logPrefix,
          address,
          domainName,
          forward.length);
      return InetSocketAddress.createUnresolved(domainName, socketAddress.getPort());
    } catch (Exception e) {
      Loggers.warnWithException(
          LOG, "[{}] Error resolving {}, returning it as-is", logPrefix, address, e);
      return socketAddress;
    }
  }

  private String lookupPtrRecord(String reversedDomain) throws Exception {
    Attributes attrs = ctx.getAttributes(reversedDomain, new String[] {"PTR"});
    for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements(); ) {
      Attribute attr = (Attribute) ae.next();
      Enumeration<?> vals = attr.getAll();
      if (vals.hasMoreElements()) {
        return vals.nextElement().toString();
      }
    }
    return null;
  }

  @Override
  public void close() {
    try {
      ctx.close();
    } catch (NamingException e) {
      Loggers.warnWithException(LOG, "Error closing translator", e);
    }
  }

  // Builds the "reversed" domain name in the ARPA domain to perform the reverse lookup
  @VisibleForTesting
  static String reverse(InetAddress address) {
    byte[] bytes = address.getAddress();
    if (bytes.length == 4) return reverseIpv4(bytes);
    else return reverseIpv6(bytes);
  }

  private static String reverseIpv4(byte[] bytes) {
    StringBuilder builder = new StringBuilder();
    for (int i = bytes.length - 1; i >= 0; i--) {
      builder.append(bytes[i] & 0xFF).append('.');
    }
    builder.append("in-addr.arpa");
    return builder.toString();
  }

  private static String reverseIpv6(byte[] bytes) {
    StringBuilder builder = new StringBuilder();
    for (int i = bytes.length - 1; i >= 0; i--) {
      byte b = bytes[i];
      int lowNibble = b & 0x0F;
      int highNibble = b >> 4 & 0x0F;
      builder
          .append(Integer.toHexString(lowNibble))
          .append('.')
          .append(Integer.toHexString(highNibble))
          .append('.');
    }
    builder.append("ip6.arpa");
    return builder.toString();
  }
}
