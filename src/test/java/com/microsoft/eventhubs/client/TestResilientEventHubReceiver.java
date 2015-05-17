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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.apache.qpid.amqp_1_0.type.messaging.MessageAnnotations;
import org.junit.Test;

public class TestResilientEventHubReceiver {
  class TestableResilientEventHubReceiver extends ResilientEventHubReceiver {
    private long receiveDelay=0;

    private int initFaultCount=0;
    private long receiveFaultOffset=-1;
    private boolean isReceiveNull=false;
    private int curInitFaults = 0;
    private boolean isFault = false;

    private long curOffset = -1;

    public TestableResilientEventHubReceiver(String connectionString,
        String eventHubName, String partitionId, String consumerGroupName,
        int defaultCredits, IEventHubFilter filter) {
      super(connectionString, eventHubName, partitionId, consumerGroupName,
          defaultCredits, filter);
    }

    //how many times initialize shall fail
    public void setInitializeFaultCount(int faultCount) {
      initFaultCount = faultCount;
    }

    //When current offset equals faultOffset, throw Runtime exception
    public void setReceiveFaultOffset(long faultOffset) {
      receiveFaultOffset = faultOffset;
    }
    
    //Simulate receive delay
    public void setReceiveDelay(long delay) {
      receiveDelay = delay;
    }
    
    public void setReceiveNull(boolean receiveNull) {
      isReceiveNull = receiveNull;
    }

    @Override
    public void initialize() throws EventHubException {
      if(curInitFaults < initFaultCount) {
        curInitFaults++;
        isFault = true;
        throw new EventHubException("Simulate fault in initialize()");
      }

      //after recovery, forget all the faults
      initFaultCount=0;
      curInitFaults=0;
      receiveFaultOffset = -1;
      isFault=false;
      isReceiveNull=false;
      curOffset=-1;
      
      if(lastMessage != null){
        String tmpOffset = EventHubMessage.parseAmqpMessage(lastMessage).getOffset();
        //make sure filter is updated
        EventHubOffsetFilter tmpFilter = new EventHubOffsetFilter(tmpOffset);
        assertEquals(filter.getFilterString(), tmpFilter.getFilterString());
        //update curOffset
        curOffset = Long.parseLong(tmpOffset);
      }
    }

    @Override
    public void close() {
      ;
    }
    
    @Override
    protected Message originalReceive(long waitTimeInMilliseconds) {
      curOffset++;
      if(isFault || curOffset == receiveFaultOffset) {
        isFault = true;
        throw new RuntimeException("Simulate fault in originalReceive()");
      }

      List<Section> sections = new ArrayList<Section>();
      Map<Object, Object> annotations = new HashMap<Object, Object>();
      annotations.put(Symbol.valueOf(Constants.OffsetKey), ""+curOffset);
      sections.add(new MessageAnnotations(annotations));
      sections.add(new Data(new Binary(("message").getBytes())));

      try{
        Thread.sleep(receiveDelay);
      }
      catch(InterruptedException e) {}
      
      if(isReceiveNull) {
        return null;
      }
      return new Message(sections);
    }
  }

  @Test
  public void testSuccessfulPath() {
    TestableResilientEventHubReceiver receiver = new TestableResilientEventHubReceiver(
        "connectionString", "name", "0", null, -1, null);
    receiver.receive(5000);
    receiver.receive(5000);
    String offset = EventHubMessage.parseAmqpMessage(receiver.receive(5000)).getOffset();
    assertEquals("2", offset);
  }

  @Test
  public void testRecovery() {
    TestableResilientEventHubReceiver receiver = new TestableResilientEventHubReceiver(
        "connectionString", "name", "0", null, -1, null);
    receiver.setReceiveFaultOffset(1);
    receiver.receive(5000); //receive message 0
    receiver.receive(5000); //receive message 1, fault, then recovered
    receiver.receive(5000); //receive message 1
    String offset = EventHubMessage.parseAmqpMessage(receiver.receive(5000)).getOffset();
    assertEquals("2", offset);
  }

  @Test
  public void testRecoveryRetries() {
    TestableResilientEventHubReceiver receiver = new TestableResilientEventHubReceiver(
        "connectionString", "name", "0", null, -1, null);
    receiver.setReceiveFaultOffset(1);
    receiver.setInitializeFaultCount(2);
    receiver.receive(5000); //receive message 0
    receiver.receive(5000); //receive message 1, fault, then recovered
    receiver.receive(5000); //receive message 1
    String offset = EventHubMessage.parseAmqpMessage(receiver.receive(5000)).getOffset();
    assertEquals("2", offset);
  }

  @Test
  public void testRecoveryBadDelays() {
    TestableResilientEventHubReceiver receiver = new TestableResilientEventHubReceiver(
        "connectionString", "name", "0", null, -1, null);
    receiver.setReceiveDelay(10);
    receiver.setReceiveNull(true);
    receiver.receive(5000); //receive null and delay is too short, recover
    receiver.receive(5000); //receive message 0
    String offset = EventHubMessage.parseAmqpMessage(receiver.receive(5000)).getOffset();
    assertEquals("1", offset);
  }
}
