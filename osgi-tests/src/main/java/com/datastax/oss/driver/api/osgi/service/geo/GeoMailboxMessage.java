/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.osgi.service.geo;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.NotThreadSafe;

/**
 * Legacy geospatial mailbox message entity retained for compatibility with OSGi test extensions.
 *
 * @deprecated DSE geospatial server operations are no longer supported.
 */
@Entity
@CqlName("messages_by_location")
@Deprecated
@NotThreadSafe
public class GeoMailboxMessage {

  public static final CqlIdentifier MAILBOX_TABLE =
      CqlIdentifier.fromInternal("messages_by_location");

  @PartitionKey @Nullable private String recipient;

  @ClusteringColumn @Nullable private Point location;

  @Nullable private String sender;

  @Nullable private String body;

  public GeoMailboxMessage() {}

  public GeoMailboxMessage(
      @NonNull String recipient,
      @NonNull Point location,
      @NonNull String sender,
      @NonNull String body) {
    this.location = location;
    this.recipient = recipient;
    this.sender = sender;
    this.body = body;
  }

  @Nullable
  public String getRecipient() {
    return recipient;
  }

  public void setRecipient(@Nullable String recipient) {
    this.recipient = recipient;
  }

  @Nullable
  public Point getLocation() {
    return location;
  }

  public void setLocation(@Nullable Point location) {
    this.location = location;
  }

  @Nullable
  public String getSender() {
    return sender;
  }

  public void setSender(@Nullable String sender) {
    this.sender = sender;
  }

  @Nullable
  public String getBody() {
    return body;
  }

  public void setBody(@Nullable String body) {
    this.body = body;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GeoMailboxMessage)) {
      return false;
    }
    GeoMailboxMessage that = (GeoMailboxMessage) o;
    return Objects.equals(recipient, that.recipient)
        && Objects.equals(location, that.location)
        && Objects.equals(sender, that.sender)
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recipient, location, sender, body);
  }
}
