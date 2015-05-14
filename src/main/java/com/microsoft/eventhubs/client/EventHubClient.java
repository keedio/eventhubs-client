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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.ConnectionErrorException;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHubClient {
  public static final String EH_SERVICE_FQDN_SUFFIX = "servicebus.windows.net";
  private static final String DefaultConsumerGroupName = "$default";
  private static final long ConnectionSyncTimeout = 60000L;
  private static final Logger logger = LoggerFactory.getLogger(EventHubClient.class);

  private final String connectionString;
  private final String entityPath;
  private final Connection connection;

  private EventHubClient(String connectionString, String entityPath) throws EventHubException {
    this.connectionString = connectionString;
    this.entityPath = entityPath;
    this.connection = this.createConnection();
  }

  /**
   * creates a new instance of EventHubClient using the supplied connection string and entity path.
   *
   * @param connectionString connection string to the namespace of event hubs. connection string format:
   * amqps://{userId}:{password}@{namespaceName}.servicebus.windows.net
   * @param entityPath the name of event hub entity.
   *
   * @return EventHubClient
   * @throws com.microsoft.eventhubs.client.EventHubException
   */
  public static EventHubClient create(String connectionString, String entityPath) throws EventHubException {
    return new EventHubClient(connectionString, entityPath);
  }

  public EventHubSender createPartitionSender(String partitionId) throws Exception {
    return new EventHubSender(this.connection.createSession(), this.entityPath, partitionId);
  }

  public EventHubConsumerGroup getConsumerGroup(String cgName) {
    if(cgName == null || cgName.length() == 0) {
      cgName = DefaultConsumerGroupName;
    }
    return new EventHubConsumerGroup(connection, entityPath, cgName);
  }

  public void close() {
    try {
      this.connection.close();
    } catch (ConnectionErrorException e) {
      logger.error(e.toString());
    }
  }
  
  public static String buildConnectionString(String username, String password, String namespace) {
    return buildConnectionString(username, password, namespace, EH_SERVICE_FQDN_SUFFIX);
  }

  public static String buildConnectionString(String username, String password,
      String namespace, String targetFqdnSuffix) {
    return "amqps://" + username + ":" + encodeString(password)
        + "@" + namespace + "." + targetFqdnSuffix;
  }
  
  private static String encodeString(String input) {
    try {
      return URLEncoder.encode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      //We don't need to throw this exception because the exception won't
      //happen because of user input. Our unit tests will catch this error.
      return "";
    }
  }

  private Connection createConnection() throws EventHubException {
    ConnectionStringParser ConnectionStringParser = new ConnectionStringParser(this.connectionString);
    Connection clientConnection;

    try {
      clientConnection = new Connection(
        ConnectionStringParser.getHost(),
        ConnectionStringParser.getPort(),
        ConnectionStringParser.getUserName(),
        ConnectionStringParser.getPassword(),
        ConnectionStringParser.getHost(),
        ConnectionStringParser.getSsl());
    } catch (ConnectionException e) {
      logger.error(e.toString());
      throw new EventHubException(e);
    }
    clientConnection.getEndpoint().setSyncTimeout(ConnectionSyncTimeout);
    SelectorFilterWriter.register(clientConnection.getEndpoint().getDescribedTypeRegistry());
    return clientConnection;
  }
}
