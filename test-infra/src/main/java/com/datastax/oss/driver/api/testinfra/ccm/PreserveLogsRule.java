package com.datastax.oss.driver.api.testinfra.ccm;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class PreserveLogsRule extends TestWatcher {
  private final CcmBridge ccmBridge;

  public PreserveLogsRule(CcmBridge ccmBridge) {
    this.ccmBridge = ccmBridge;
  }

  @Override
  protected void failed(Throwable e, Description description) {
    ccmBridge.setKeepLogs(true);
  }
}
