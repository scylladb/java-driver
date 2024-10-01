package com.datastax.oss.driver.core.resolver;

import org.burningwave.tools.net.DefaultHostResolver;
import org.burningwave.tools.net.HostResolutionRequestInterceptor;

public class MultimapHostResolverProvider {
  private static volatile MultimapHostResolver resolver = null;

  private MultimapHostResolverProvider() {}

  public static synchronized void setResolver(MultimapHostResolver newResolver) {
    if (resolver != null) {
      throw new IllegalStateException("Resolver is already set. Cannot set new.");
    }
    resolver = newResolver;
    HostResolutionRequestInterceptor.INSTANCE.install(resolver, DefaultHostResolver.INSTANCE);
  }

  public static synchronized void addResolverEntry(String hostname, String address) {
    if (resolver == null) {
      setResolver(new MultimapHostResolver());
    }
    resolver.putHost(hostname, address);
  }

  public static synchronized void removeResolverEntries(String hostname) {
    if (resolver == null) {
      return;
    }
    resolver.removeHost(hostname);
  }
}
