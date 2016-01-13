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
package com.microsoft.eventhubs.client.example;

import com.microsoft.eventhubs.client.*;

import java.io.IOException;

/**
 * An example showing how to use HadoopOffsetAwareResilientEventHubReceiver to receive messages from
 * Eventhubs.
 */
public class OffsetAwareResilientEventHubReceiveClient {
  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 7) {
      System.out.println("Usage: ReceiveClient <policyName> <policyKey> <namespace> <name> <partitionId> <consumerGroup> <checkpointDir>");
      return;
    }
    
    String policyName = args[0];
    String policyKey = args[1];
    String namespace = args[2];
    String name = args[3];
    String partitionId = args[4];
    String consumerGroup = args[5];
    String checkpointDir = args[6];

    String connectionString = new ConnectionStringBuilder(policyName, policyKey, namespace).getConnectionString();

    ResilientEventHubReceiver receiver = new HadoopOffsetAwareResilientEventHubReceiver(
        connectionString, name, partitionId, consumerGroup, -1, checkpointDir);

    try {
      receiver.initialize();
    }
    catch(EventHubException e) {
      System.out.println("Exception: " + e.getMessage());
      return;
    }
    
    while(true) {
      EventHubMessage message = EventHubMessage.parseAmqpMessage(receiver.receive(5000));
      if(message != null) {
        System.out.println("Received: (" + message.getOffset() + " | "
            + message.getSequence() + " | " + message.getEnqueuedTimestamp()
            + ") => " + message.getDataAsString());
      }
    }
  }
}
