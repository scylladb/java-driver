package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;

public abstract class SessionRegistry {
  public SessionRegistry() {
    DefaultDriverContext.setSessionRegistry(this);
  }

  public abstract void registerSession(Object session);

  public abstract void closeSession(Object session);
}
