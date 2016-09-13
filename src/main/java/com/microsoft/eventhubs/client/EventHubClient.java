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

import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.ConnectionErrorException;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubClient {
  private static final Logger logger = LoggerFactory.getLogger(EventHubClient.class);

  private final String connectionString;
  private final String entityPath;
  private final Connection connection;

  private EventHubClient(String connectionString, String entityPath) throws EventHubException {
    this.connectionString = connectionString;
    this.entityPath = entityPath;
    connection = createConnection(connectionString);
  }
  
  String getConnectionString() {
    return connectionString;
  }
  
  String getEntityPath() {
    return entityPath;
  }

  /**
   * creates a new instance of EventHubClient using the supplied connection string and entity path.
   *
   * connection string format:
   * amqps://{username}:{password}@{namespace}.{serviceFqdnSuffix}
   * name is the name of event hub entity.
   */
  public static EventHubClient create(String connectionString, String name)
      throws EventHubException {
    return new EventHubClient(connectionString, name);
  }
  
  /**
   * creates a new instance of EventHubClient using individual fields such as
   * policyName, policyKey, namespace etc.
   */
  public static EventHubClient create(String policyName, String policyKey,
      String namespace, String name)
          throws EventHubException {
    String connectionString = new ConnectionStringBuilder(policyName, policyKey,
        namespace).getConnectionString();
    return new EventHubClient(connectionString, name);
  }

  public EventHubSender createPartitionSender(String partitionId) throws EventHubException {
      return new EventHubSender(connectionString, entityPath, partitionId);
  }

  public EventHubConsumerGroup getConsumerGroup(String cgName) {
    if(cgName == null || cgName.length() == 0) {
      cgName = Constants.DefaultConsumerGroupName;
    }
    return new EventHubConsumerGroup(connection, entityPath, cgName);
  }

  public void close() {
    try {
      connection.close();
    } catch (ConnectionErrorException e) {
      logger.error("An error occured while closing the connection", e);
    }
  }

  public static Connection createConnection(String connectionString) throws EventHubException {
    ConnectionStringParser ConnectionStringParser = new ConnectionStringParser(connectionString);
    Connection clientConnection;

    try {
      clientConnection = new Connection(
        ConnectionStringParser.getHost(),
        ConnectionStringParser.getPort(),
        ConnectionStringParser.getPolicyName(),
        ConnectionStringParser.getPolicyKey(),
        ConnectionStringParser.getHost(),
        ConnectionStringParser.getSsl());
    } catch (ConnectionException e) {
      throw new EventHubException(e);
    }
    clientConnection.getEndpoint().setSyncTimeout(Constants.ConnectionSyncTimeout);
    SelectorFilterWriter.register(clientConnection.getEndpoint().getDescribedTypeRegistry());
    return clientConnection;
  }
}
