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

import java.util.Date;
import java.util.Map;

import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.messaging.AmqpValue;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.apache.qpid.amqp_1_0.type.messaging.MessageAnnotations;

public class EventHubMessage {
  private String offset;
  private long sequence;
  private long enqueuedTimestamp;
  private String data;

  public EventHubMessage(String offset, long sequence, long enqueuedTimestamp, String data) {
    this.offset = offset;
    this.sequence = sequence;
    this.enqueuedTimestamp = enqueuedTimestamp;
    this.data = data;
  }

  public static EventHubMessage parseAmqpMessage(Message message) {
    EventHubMessage ehMessage = null;
    
    if(message != null) {
      String offset = null;
      long sequence = 0;
      long enqueuedTimestamp = 0;
      String data = null;
      for (Section section : message.getPayload()) {
        if (section instanceof MessageAnnotations) {
          Map annotationMap = ((MessageAnnotations)section).getValue();
  
          if (annotationMap.containsKey(Symbol.valueOf(Constants.OffsetKey))) {
            offset = (String) annotationMap.get(
                Symbol.valueOf(Constants.OffsetKey));
          }
          if (annotationMap.containsKey(
              Symbol.valueOf(Constants.SequenceNumberKey))) {
            sequence = (Long) annotationMap.get(
                Symbol.valueOf(Constants.SequenceNumberKey));
          }
          if (annotationMap.containsKey(
              Symbol.valueOf(Constants.EnqueuedTimeKey))) {
            enqueuedTimestamp = ((Date) annotationMap.get(
                Symbol.valueOf(Constants.EnqueuedTimeKey))).getTime();
          }
        }
        else if (data == null && section instanceof Data) {
          data = new String(((Data)section).getValue().getArray());
        }
        else if (data == null && section instanceof AmqpValue) {
          data = ((AmqpValue) section).getValue().toString();
        }
      }
      ehMessage = new EventHubMessage(offset, sequence, enqueuedTimestamp, data);
    }
    return ehMessage;
  }

  /**
   * Get offset of the message, offset can be used as filter to create
   * EventHubReceiver.
   */
  public String getOffset() {
    return offset;
  }

  /**
   * Get sequence number of the message, sequence number cannot be used as
   * filter to create EventHubReceiver.
   * Sequence number can be used to reliably compare the order of the messages.
   */
  public long getSequence() {
    return sequence;
  }

  /**
   * Get enqueued time of the message, enqueued time can be used as filter
   * to create EventHubReceiver.
   */
  public long getEnqueuedTimestamp() {
    return enqueuedTimestamp;
  }

  /**
   * Get the raw data of the message.
   */
  public String getData() {
    return data;
  }
}
