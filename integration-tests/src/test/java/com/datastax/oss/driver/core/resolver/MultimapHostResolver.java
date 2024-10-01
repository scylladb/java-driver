package com.datastax.oss.driver.core.resolver;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.burningwave.tools.net.HostResolutionRequestInterceptor;
import org.burningwave.tools.net.HostResolver;
import org.burningwave.tools.net.IPAddressUtil;

/** Variation on MappedHostResolver that uses Multimap instead. Allows duplicate entries. */
public class MultimapHostResolver implements HostResolver {
  protected Multimap<String, String> hostAliases;

  public MultimapHostResolver(Multimap<String, String> hostAliases) {
    this.hostAliases = hostAliases;
  }

  public MultimapHostResolver() {
    this.hostAliases = ArrayListMultimap.create();
  }

  @Override
  public Collection<InetAddress> getAllAddressesForHostName(Map<String, Object> argumentMap) {
    String hostName = (String) getMethodArguments(argumentMap)[0];
    List<InetAddress> inetAddresses = new ArrayList<>();

    if (hostAliases.containsKey(hostName)) {
      Collection<String> iPAddresses = hostAliases.get(hostName);
      try {
        for (String iPAddress : iPAddresses) {
          inetAddresses.add(
              InetAddress.getByAddress(
                  hostName, IPAddressUtil.INSTANCE.textToNumericFormat(iPAddress)));
        }
      } catch (UnknownHostException e) {

      }
    }
    return inetAddresses;
  }

  @Override
  public Collection<String> getAllHostNamesForHostAddress(Map<String, Object> argumentMap) {
    byte[] address = (byte[]) getMethodArguments(argumentMap)[0];
    Collection<String> hostNames = new ArrayList<>();
    String iPAddress = IPAddressUtil.INSTANCE.numericToTextFormat(address);
    for (Map.Entry<String, String> entry : hostAliases.entries()) {
      if (entry.getValue().equals(iPAddress)) {
        hostNames.add(entry.getKey());
        break;
      }
    }
    return hostNames;
  }

  public synchronized MultimapHostResolver putHost(String hostname, String iP) {
    Multimap<String, String> hostAliases = ArrayListMultimap.create(this.hostAliases);
    hostAliases.put(hostname, iP);
    this.hostAliases = hostAliases;
    return this;
  }

  public synchronized MultimapHostResolver removeHost(String hostname) {
    Multimap<String, String> hostAliases = ArrayListMultimap.create(this.hostAliases);
    hostAliases.removeAll(hostname);
    this.hostAliases = hostAliases;
    return this;
  }

  public synchronized MultimapHostResolver removeHostForIP(String iP) {
    Multimap<String, String> hostAliases = ArrayListMultimap.create(this.hostAliases);
    Iterator<Map.Entry<String, String>> hostAliasesIterator = hostAliases.entries().iterator();
    while (hostAliasesIterator.hasNext()) {
      Map.Entry<String, String> host = hostAliasesIterator.next();
      if (host.getValue().contains(iP)) {
        hostAliasesIterator.remove();
      }
    }
    this.hostAliases = hostAliases;
    return this;
  }

  @Override
  public boolean isReady(HostResolutionRequestInterceptor hostResolverService) {
    return HostResolver.super.isReady(hostResolverService) && obtainsResponseForMappedHost();
  }

  protected synchronized boolean obtainsResponseForMappedHost() {
    String hostNameForTest = null;
    if (hostAliases.isEmpty()) {
      putHost(hostNameForTest = UUID.randomUUID().toString(), "127.0.0.1");
    }
    try {
      for (String hostname : hostAliases.keySet()) {
        InetAddress.getByName(hostname);
      }
      return true;
    } catch (UnknownHostException exc) {
      return false;
    } finally {
      if (hostNameForTest != null) {
        removeHost(hostNameForTest);
      }
    }
  }
}
