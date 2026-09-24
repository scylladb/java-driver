package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.metadata.token.Token;

/** Holds sharding information for a particular Node. */
public interface NodeShardingInfo {

  public int getShardsCount();

  /**
   * Returns a shardId for a given Token.
   *
   * <p>Accepts all types of Tokens but if the Token is not an instance of {@link
   * com.datastax.oss.driver.internal.core.metadata.token.TokenLong64} then the return value could
   * be not meaningful (e.g. random shard). This method does not verify if the given Token belongs
   * to the Node.
   */
  public int shardId(Token t);

  /**
   * Returns shard aware port number where Scylla is listening for CQL connections. If present, it
   * works almost the same way as port 9042 typically does; the difference is that client-side port
   * number is used as an indicator to which shard client wants to connect.
   */
  public Integer getShardAwarePort();

  /**
   * Returns shard aware SSL port number where Scylla is listening for encrypted CQL connections. If
   * present, it works almost the same way as port 9142 typically does; the difference is that
   * client-side port number is used as an indicator to which shard client wants to connect.
   */
  public Integer getShardAwarePortSsl();
}
