package com.datastax.oss.driver.api.core.metadata.token;

import java.nio.ByteBuffer;

/** Allows to hash partition key to a @code{Token}. */
public interface Partitioner {
  Token hash(ByteBuffer partitionKey);
}
