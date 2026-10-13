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
package com.datastax.oss.driver.api.osgi.service.graph;

import com.datastax.oss.driver.api.osgi.service.MailboxException;
import com.datastax.oss.driver.api.osgi.service.MailboxMessage;
import com.datastax.oss.driver.api.osgi.service.MailboxService;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A compatibility interface for mailbox services that supported DSE Graph.
 *
 * @deprecated DSE Graph mailbox services are no longer supported.
 */
@Deprecated
public interface GraphMailboxService extends MailboxService {

  /**
   * Sends a message through a Graph mailbox.
   *
   * @throws UnsupportedOperationException always in the default implementation
   * @deprecated DSE Graph mailbox services are no longer supported. Use {@link
   *     MailboxService#sendMessage(MailboxMessage)} for non-Graph mailboxes.
   */
  @Deprecated
  default void sendGraphMessage(@NonNull MailboxMessage message) throws MailboxException {
    throw new UnsupportedOperationException("DSE Graph mailbox services are no longer supported");
  }

  /**
   * Retrieves Graph messages for a recipient.
   *
   * @throws UnsupportedOperationException always in the default implementation
   * @deprecated DSE Graph mailbox services are no longer supported. Use {@link
   *     MailboxService#getMessages(String)} for non-Graph mailboxes.
   */
  @Deprecated
  @NonNull
  default Iterable<MailboxMessage> getGraphMessages(@NonNull String recipient)
      throws MailboxException {
    throw new UnsupportedOperationException("DSE Graph mailbox services are no longer supported");
  }
}
