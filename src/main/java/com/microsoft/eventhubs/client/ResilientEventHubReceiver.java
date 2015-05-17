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

import org.apache.qpid.amqp_1_0.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A resilient wrapper of EventHubReceiver which automatically reconnects to
 * EventHubs in the case of error.
 */
public class ResilientEventHubReceiver {
  private static final Logger logger = LoggerFactory.getLogger(EventHubClient.class);
  public static final int RecoveryRetryCount = 3;
  public static final int RecoveryRetryInterval = 1000;
  
  protected String connectionString;
  protected String eventHubName;
  protected String partitionId;
  protected String consumerGroupName;
  protected int defaultCredits;
  protected IEventHubFilter filter;
  
  private EventHubClient client;
  private EventHubConsumerGroup consumerGroup;
  private EventHubReceiver receiver;
  protected Message lastMessage; //need to use offset of this message to recover

  public ResilientEventHubReceiver(String connectionString,
      String eventHubName,
      String partitionId,
      String consumerGroupName,
      int defaultCredits,
      IEventHubFilter filter) {
    this.connectionString = connectionString;
    this.eventHubName = eventHubName;
    this.partitionId = partitionId;
    this.consumerGroupName = consumerGroupName;
    this.defaultCredits = defaultCredits;
    this.filter = filter;
  }
  
  public void initialize() throws EventHubException {
    if(client == null) {
      client = EventHubClient.create(connectionString, eventHubName);
      consumerGroup = client.getConsumerGroup(consumerGroupName);
      receiver = consumerGroup.createReceiver(partitionId, filter, defaultCredits);
    }
  }

  public void close() {
    if(receiver != null) {
      receiver.close();
      receiver = null;
    }

    if(consumerGroup != null) {
      consumerGroup.close();
      consumerGroup = null;
    }

    if(client != null) {
      client.close();
      client = null;
    }
  }
  
  protected Message originalReceive(long waitTimeInMilliseconds) {
    return receiver.receive(waitTimeInMilliseconds);
  }

  public Message receive(long waitTimeInMilliseconds) {
    Message message = null;
    try {
      long start = System.currentTimeMillis();
      message = originalReceive(waitTimeInMilliseconds);
      long end = System.currentTimeMillis();
      long millis = (end - start);
      if (message == null) {
        //Temporary workaround for AMQP/EH bug of failing to receive messages
        if(waitTimeInMilliseconds > 100 && millis < waitTimeInMilliseconds/2) {
          logger.error("Failed to receive messages in " + millis
              + " millisecond. Recovering.");
          throw new Exception(); //will be caught later and recover()
        }
      }
      else {
        lastMessage = message;
      }
    }
    catch(Exception e) {
      recover();
    }
    return message;
  }

  public void recover() {
    if(lastMessage != null) {
      //change original filter to offset filter based on last message
      EventHubMessage ehMessage = EventHubMessage.parseAmqpMessage(lastMessage);
      logger.info("Recovering with offset filter " + ehMessage.getOffset());
      filter = new EventHubOffsetFilter(ehMessage.getOffset());
    }
    int retries = 0;
    while(retries < RecoveryRetryCount) {
      close();

      try{
        Thread.sleep(RecoveryRetryInterval);
      }
      catch(InterruptedException e) {
      }

      try {
        initialize();
      }
      catch(EventHubException e) {
        logger.warn("Failed to recover, current retry " + retries);
        retries++;
        continue;
      }
      break;
    }
    if(retries < RecoveryRetryCount) {
      logger.info("Successfully recovered");
    }
    else {
      logger.error("Failed to recover");
      //TBD: should we throw RuntimeException?
    }
  }
}
