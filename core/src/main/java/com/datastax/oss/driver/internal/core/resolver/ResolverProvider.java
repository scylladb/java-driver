package com.datastax.oss.driver.internal.core.resolver;

import com.datastax.oss.driver.internal.core.resolver.defaultResolver.DefaultResolverFactory;

/**
 * Entry point for driver components to getting the {@link Resolver} instances. By default returns
 * instances of {@link DefaultResolverFactory}.
 */
public class ResolverProvider {

  private static boolean alreadyInUse = false;
  private static boolean alreadySet = false;
  private static AbstractResolverFactory defaultResolverFactoryImpl = new DefaultResolverFactory();

  /**
   * Asks factory for new {@link Resolver}.
   *
   * @param clazz Class that is requesting the {@link Resolver}.
   * @return new {@link Resolver}.
   */
  public static synchronized Resolver getResolver(Class<?> clazz) {
    alreadyInUse = true;
    return defaultResolverFactoryImpl.getResolver(clazz);
  }

  /**
   * Replaces resolver factory with another, possibly producing different implementation of {@link
   * Resolver}.
   *
   * @param resolverFactoryImpl new {@link Resolver} factory.
   */
  public static synchronized void setDefaultResolverFactory(
      AbstractResolverFactory resolverFactoryImpl) {
    if (alreadyInUse) {
      throw new IllegalStateException(
          "Cannot change default resolver factory: ResolverProvider has already returned "
              + "an instance of a Resolver to use. Default resolver factory needs to be set up before first use by any "
              + "class.");
    }
    if (alreadySet) {
      throw new IllegalStateException(
          "Cannot change default resolver factory: this method has already been called. "
              + "You can set default resolver factory only once.");
    }
    alreadySet = true;
    defaultResolverFactoryImpl = resolverFactoryImpl;
  }
}
