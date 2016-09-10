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
import org.apache.qpid.amqp_1_0.client.ConnectionException;
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
  private static final int retryCount = 3;
  private static final int retryDelayInMilliseconds = 1000;

  private final String connectionString;
  private final String entityPath;
  private final String partitionId;
  private final String destinationAddress;

  private Session session;
  private Sender sender;
  private boolean shouldRecreateSession;

  @Deprecated
  public EventHubSender(Session session, String entityPath, String partitionId) {
    this.connectionString = null;
    this.session = session;
    this.entityPath = entityPath;
    this.partitionId = partitionId;
    this.destinationAddress = getDestinationAddress();
  }

  public EventHubSender(String connectionString, String entityPath, String partitionId) {
    this.connectionString = connectionString;
    this.entityPath = entityPath;
    this.partitionId = partitionId;
    this.destinationAddress = getDestinationAddress();
    this.shouldRecreateSession = true;
  }

  public void send(Section section) throws EventHubException {
      Message message = new Message(section);
      sendCore(message);
  }
  
  public void send(byte[] data) throws EventHubException {
      Binary bin = new Binary(data);
      Message message = new Message(new Data(bin));
      sendCore(message);
  }

  public void send(String data) throws EventHubException {
    //For interop with other language, convert string to bytes
    send(data.getBytes());
  }
	
  private void sendCore(Message message) throws EventHubException {
    int retry = 0;
    boolean sendSucceeded = false;

    while(!sendSucceeded) {
      try {
        ensureSenderCreated();
        sender.send(message);
        sendSucceeded = true;
      } catch(Exception e) {
        HandleException(e);
        
        if(++retry > retryCount) {
          throw new EventHubException("An error occurred while sending data.", e);
        }
      }
    }
  }

  private void HandleException(Exception e) throws EventHubException {
    logger.error(e.getMessage());
    if(e instanceof LinkDetachedException) {
      //We want to re-establish the connection if the sender was closed
      //The sender.isClosed() does not get set properly if LinkDetachedException occurs
      //Hence let's set it explicitly to null, if the caller is just eating the exceptions (for e.g. storm-eventhubs bolt)
      //TODO: We may have to do the same for more exception types which may require re-open
      //For now this is good enough for long running client sending objects continuously
      sender = null;
    } else if (e instanceof ConnectionException)
    {
      // We need to recreate a connection and session to recover from the failure.
      shouldRecreateSession = true;
      sender = null;
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

      if(connectionString != null && shouldRecreateSession)
      {
        session = EventHubClient.createConnection(connectionString).createSession();
        shouldRecreateSession = false;
      }

      sender = session.createSender(destinationAddress);
    }
  }
}
