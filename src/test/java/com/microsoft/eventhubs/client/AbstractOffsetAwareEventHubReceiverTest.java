package com.microsoft.eventhubs.client;

import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.apache.qpid.amqp_1_0.type.messaging.MessageAnnotations;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 13/1/16.
 */
public abstract class AbstractOffsetAwareEventHubReceiverTest {
  static Message fakeOffsetWrapperMessage(String offSet) {
    Map<Symbol,String> annotationMap = new HashMap<>();
    annotationMap.put(Symbol.valueOf(Constants.OffsetKey), offSet);
    MessageAnnotations annotations = new MessageAnnotations(annotationMap);
    Data fakeData = new Data(new Binary("".getBytes()));
    List<Section> sections = new ArrayList<>();
    sections.add(fakeData);
    sections.add(annotations);

    return new Message(sections);
  }

  @Test
  public void testFakeMessageGenerator(){
    String offset = "338422478789101";
    Message fakeMessage = fakeOffsetWrapperMessage(offset);

    EventHubMessage ehMsg = EventHubMessage.parseAmqpMessage(fakeMessage);
    assertEquals(offset, ehMsg.getOffset());
  }

}
