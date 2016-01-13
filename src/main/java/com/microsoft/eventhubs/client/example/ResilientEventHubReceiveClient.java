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

import com.microsoft.eventhubs.client.ConnectionStringBuilder;
import com.microsoft.eventhubs.client.EventHubEnqueueTimeFilter;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubMessage;
import com.microsoft.eventhubs.client.IEventHubFilter;
import com.microsoft.eventhubs.client.ResilientEventHubReceiver;

/**
 * An example showing how to use ResilientEventHubReceiver to receive messages from
 * Eventhubs.
 */
public class ResilientEventHubReceiveClient {
  public static void main(String[] args) {
    if (args == null || args.length < 6) {
      System.out.println("Usage: ReceiveClient <policyName> <policyKey> <namespace> <name> <partitionId> <consumerGroup> [timeFilterDiff]");
      return;
    }
    
    String policyName = args[0];
    String policyKey = args[1];
    String namespace = args[2];
    String name = args[3];
    String partitionId = args[4];
    String consumerGroup = args[5];
    long enqueueTime = 0;
    if(args.length >= 7) {
      long enqueueTimeDiff = Integer.parseInt(args[6]);
      enqueueTime = System.currentTimeMillis() - enqueueTimeDiff*1000;
    }

    String connectionString = new ConnectionStringBuilder(policyName, policyKey, namespace).getConnectionString();
    IEventHubFilter filter = null;
    if(enqueueTime != 0) {
      filter = new EventHubEnqueueTimeFilter(enqueueTime);
    }
    
    ResilientEventHubReceiver receiver = new ResilientEventHubReceiver(
        connectionString, name, partitionId, consumerGroup, -1, filter);

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
