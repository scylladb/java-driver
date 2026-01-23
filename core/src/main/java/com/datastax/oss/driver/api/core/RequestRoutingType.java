package com.datastax.oss.driver.api.core;

/** The type of routing for a given request. */
public enum RequestRoutingType {
  /** A regular (non-LWT) request. */
  REGULAR,
  /** A lightweight transaction (LWT) request. */
  LWT
}
