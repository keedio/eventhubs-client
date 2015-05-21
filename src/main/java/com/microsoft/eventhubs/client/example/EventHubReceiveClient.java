package com.microsoft.eventhubs.client.example;

import com.microsoft.eventhubs.client.Constants;
import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubEnqueueTimeFilter;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubMessage;
import com.microsoft.eventhubs.client.EventHubReceiver;

/**
 * An example showing how to use EventHubReceiver to receive messages from
 * Eventhubs.
 */
public class EventHubReceiveClient {
  public static void main(String[] args) {
    if (args == null || args.length < 5) {
      System.out.println("Usage: ReceiveClient <policyName> <policyKey> <namespace> <name> <partitionId> [timeFilterDiff]");
      return;
    }
    
    String policyName = args[0];
    String policyKey = args[1];
    String namespace = args[2];
    String name = args[3];
    String partitionId = args[4];
    long enqueueTime = 0;
    if(args.length >= 6) {
      long enqueueTimeDiff = Integer.parseInt(args[5]);
      enqueueTime = System.currentTimeMillis() - enqueueTimeDiff*1000;
    }
    
    try {
      
      EventHubClient client = EventHubClient.create(policyName, policyKey, namespace, name);
      EventHubReceiver receiver = null;
      if(enqueueTime == 0) {
        receiver = client.getConsumerGroup(null).createReceiver(partitionId,
            null, Constants.DefaultAmqpCredits);
      }
      else {
        receiver = client.getConsumerGroup(null).createReceiver(partitionId,
            new EventHubEnqueueTimeFilter(enqueueTime), Constants.DefaultAmqpCredits);
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
    catch(EventHubException e) {
      System.out.println("Exception: " + e.getMessage());
    }
  }
}
