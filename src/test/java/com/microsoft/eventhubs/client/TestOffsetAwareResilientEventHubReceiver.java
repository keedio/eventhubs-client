package com.microsoft.eventhubs.client;

import org.apache.qpid.amqp_1_0.client.Message;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;


/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 12/1/16.
 */
public class TestOffsetAwareResilientEventHubReceiver extends AbstractOffsetAwareEventHubReceiverTest {
  private static final Logger logger = LoggerFactory.getLogger(TestOffsetAwareResilientEventHubReceiver.class);
  class TestableOffsetAwareResilientEventHubReceiver extends OffsetAwareResilientEventHubReceiver {
    public TestableOffsetAwareResilientEventHubReceiver(String connectionString,
                                                        String eventHubName, String partitionId, String consumerGroupName,
                                                        int defaultCredits,
                                                        OffsetResolverFunction offsetResolver,
                                                        OffsetDumperFunction offsetDumper,
                                                        Message fakeMessage) {
      super(connectionString, eventHubName, partitionId, consumerGroupName,
              defaultCredits, offsetResolver, offsetDumper);
      lastMessage = fakeMessage;
    }
  }

  @Test
  public void testOffsetDump(){
    Map<String, String> offsetMemoryStore = new HashMap<>();
    Message fakeMessage = fakeOffsetWrapperMessage("338422478789101");

    TestableOffsetAwareResilientEventHubReceiver receiver0 = new TestableOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1,
            null,
            (_offset, _partitionId, _consumerGroup) -> offsetMemoryStore.put(_partitionId + "#" + _consumerGroup, _offset),
            fakeMessage);

    assertFalse(offsetMemoryStore.containsKey("0#myConsumerGroupName"));

    receiver0.close();
    assertTrue(offsetMemoryStore.containsKey("0#myConsumerGroupName"));
    assertEquals("338422478789101",offsetMemoryStore.get("0#myConsumerGroupName"));
  }

  @Test
  public void testSuccessfulOffsetRetrieval(){
    Map<String, String> offsetMemoryStore = new HashMap<>();
    Message fakeMessage = fakeOffsetWrapperMessage("338422478789101");

    TestableOffsetAwareResilientEventHubReceiver receiver0 = new TestableOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1,
            null,
            (_offset, _partitionId, _consumerGroup) -> offsetMemoryStore.put(_partitionId + "#" + _consumerGroup, _offset),
            fakeMessage);

    assertFalse(offsetMemoryStore.containsKey("0#myConsumerGroupName"));

    receiver0.close();
    assertTrue(offsetMemoryStore.containsKey("0#myConsumerGroupName"));
    assertEquals("338422478789101",offsetMemoryStore.get("0#myConsumerGroupName"));

    OffsetAwareResilientEventHubReceiver receiver1 = new OffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1,
            (_partitionId, _consumerGroup) -> Optional.of(offsetMemoryStore.get(_partitionId + "#" + _consumerGroup)),
            null);

    assertNotNull(receiver1.filter);

    assertEquals("338422478789101", receiver1.filter.getFilterValue());
  }

  @Test(expected = RuntimeException.class)
  public void testUnsuccesfulOffsetDumper(){
    logger.info("Testing unsuccessful offset dump, following exception is OK");

    Message fakeMessage = fakeOffsetWrapperMessage("338422478789101");

    TestableOffsetAwareResilientEventHubReceiver receiver0 = new TestableOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1,
            null,
            (_offset, _partitionId, _consumerGroup) -> { throw new IOException("test exception"); },
            fakeMessage);

    receiver0.close();
  }
}
