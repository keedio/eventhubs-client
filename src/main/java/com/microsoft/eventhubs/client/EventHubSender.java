/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.microsoft.eventhubs.client;

import java.util.concurrent.TimeoutException;
import org.apache.qpid.amqp_1_0.client.LinkDetachedException;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.client.Sender;
import org.apache.qpid.amqp_1_0.client.Session;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubSender {

  private static final Logger logger = LoggerFactory.getLogger(EventHubSender.class);

  private final Session session;
  private final String entityPath;
  private final String partitionId;
  private final String destinationAddress;

  private Sender sender;

  public EventHubSender(Session session, String entityPath, String partitionId) {
    this.session = session;
    this.entityPath = entityPath;
    this.partitionId = partitionId;
    this.destinationAddress = getDestinationAddress();
  }
  
  public void send(Section section) throws EventHubException {
    try {
      ensureSenderCreated();

      Message message = new Message(section);
      sender.send(message);

    } catch (Exception e) {
        HandleException(e);
    }
  }
  
  public void send(byte[] data) throws EventHubException {
    try {
      ensureSenderCreated();

      Binary bin = new Binary(data);
      Message message = new Message(new Data(bin));
      sender.send(message);

    } catch (Exception e) {
      HandleException(e);
    }
  }

  public void send(String data) throws EventHubException {
    //For interop with other language, convert string to bytes
    send(data.getBytes());
  }

  public void HandleException(Exception e) throws EventHubException {
    logger.error(e.getMessage());
    if(e instanceof LinkDetachedException) {
      //We want to re-establish the connection if the sender was closed
      //The sender.isClosed() does not get set properly if LinkDetachedException occurs
      //Hence let's set it explicitly to null, if the caller is just eating the exceptions (for e.g. storm-eventhubs bolt)
      //TODO: We may have to do the same for more exception types which may require re-open
      //For now this is good enough for long running client sending objects continuously
      sender = null;
      throw new EventHubException("Sender has been closed.", e);
    } else if (e instanceof TimeoutException) {
      throw new EventHubException("Timed out while waiting to get credit to send.", e);
    } else {
      throw new EventHubException("An unexpected error occurred while sending data.", e);
    }
  }
  public void close() throws EventHubException {
    try {
      sender.close();
    } catch (Sender.SenderClosingException e) {
      throw new EventHubException("An error occurred while closing the sender.", e);
    }
  }

  private String getDestinationAddress() {
    if (partitionId == null || partitionId.equals("")) {
      return entityPath;
    } else {
      return String.format(Constants.DestinationAddressFormatString, entityPath, partitionId);
    }
  }

  private synchronized void ensureSenderCreated() throws Exception {
    if (sender == null || sender.isClosed()) {
      logger.info("Creating EventHubs sender");
      sender = session.createSender(destinationAddress);
    }
  }
}
