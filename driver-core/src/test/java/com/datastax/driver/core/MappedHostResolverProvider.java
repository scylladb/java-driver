package com.datastax.driver.core;

import org.burningwave.tools.net.DefaultHostResolver;
import org.burningwave.tools.net.HostResolutionRequestInterceptor;
import org.burningwave.tools.net.MappedHostResolver;

public class MappedHostResolverProvider {
  private static volatile MappedHostResolver resolver = null;

  private MappedHostResolverProvider() {}

  public static synchronized boolean setResolver(MappedHostResolver newResolver) {
    if (resolver != null) {
      return false;
    }
    resolver = newResolver;
    HostResolutionRequestInterceptor.INSTANCE.install(resolver, DefaultHostResolver.INSTANCE);
    return true;
  }

  public static synchronized void addResolverEntry(String hostname, String address) {
    if (resolver == null) {
      setResolver(new MappedHostResolver());
    }
    resolver.putHost(hostname, address);
  }

  public static synchronized void removeResolverEntry(String hostname) {
    if (resolver == null) {
      return;
    }
    resolver.removeHost(hostname);
  }
}
