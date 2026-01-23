package com.datastax.oss.driver.api.core;

public enum RequestRoutingMethod {
  REGULAR,
  PRESERVE_REPLICA_ORDER,
  TOKEN_BASED_REPLICA_SHUFFLING
}
