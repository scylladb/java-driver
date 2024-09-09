package com.datastax.oss.driver.internal.core.resolver;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.datastax.oss.driver.internal.core.resolver.mockResolver.MockResolverFactory;
import org.junit.Before;
import org.junit.Test;

public class ResolverProviderTest {

  @Before
  public void resetState() {
    ResolverProvider.resetBooleansForTesting();
  }

  @Test(expected = IllegalStateException.class)
  public void should_not_allow_setting_twice() {
    try {
      MockResolverFactory first = new MockResolverFactory();
      MockResolverFactory second = new MockResolverFactory();
      ResolverProvider.setDefaultResolverFactory(first);
      ResolverProvider.setDefaultResolverFactory(second);
    } catch (IllegalStateException ex) {
      assertThat(ex.getMessage())
          .isEqualTo(
              "Cannot change default resolver factory: this method has already been called. You can set "
                  + "default resolver factory only once.");
      throw ex;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void should_not_allow_setting_once_in_use() {
    try {
      ResolverProvider.getResolver(ResolverProviderTest.class);
      MockResolverFactory first = new MockResolverFactory();
      ResolverProvider.setDefaultResolverFactory(first);
    } catch (IllegalStateException ex) {
      assertThat(ex.getMessage())
          .isEqualTo(
              "Cannot change default resolver factory: ResolverProvider has already returned an instance of a "
                  + "Resolver to use. Default resolver factory needs to be set up before first use by any class.");
      throw ex;
    }
  }

  @Test
  public void should_allow_setting_once() {
    MockResolverFactory first = new MockResolverFactory();
    ResolverProvider.setDefaultResolverFactory(first);
  }
}
