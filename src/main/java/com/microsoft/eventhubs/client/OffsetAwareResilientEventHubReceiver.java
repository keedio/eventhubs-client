package com.microsoft.eventhubs.client;

import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.apache.qpid.amqp_1_0.type.messaging.MessageAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Event Hub receiver able to handle persistence and retrieval of the last offset received from event hub.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 12/1/16.
 */
public class OffsetAwareResilientEventHubReceiver extends ResilientEventHubReceiver {
  private static final Logger logger = LoggerFactory.getLogger(OffsetAwareResilientEventHubReceiver.class);

  protected OffsetDumperFunction offsetDumper;

  /**
   * Constructs an event hub receiver using the user provided functions to resolve
   * and persist the last offset received from event hub.
   *
   * @param connectionString event hub connection string.
   * @param eventHubName event hub name.
   * @param partitionId partition id to listen.
   * @param consumerGroupName the name of the consumer group this receiver belongs to.
   * @param defaultCredits number of credits.
   * @param offsetResolver function to use as a resolver for an offset stored in an external data store.
   * @param offsetDumper function to use to store the last offset stored in an external data store.
   */
  public OffsetAwareResilientEventHubReceiver(String connectionString,
                                              String eventHubName,
                                              String partitionId,
                                              String consumerGroupName,
                                              int defaultCredits,
                                              OffsetResolverFunction offsetResolver,
                                              OffsetDumperFunction offsetDumper) {

    super(connectionString, eventHubName, partitionId, consumerGroupName, defaultCredits, null);
    this.offsetDumper = offsetDumper;

    if (offsetResolver != null){
      Optional<String> offSet = offsetResolver.apply(partitionId, consumerGroupName);

      if (offSet.isPresent()) {
        filter = new EventHubOffsetFilter(offSet.get());
      }
    }
  }

  @Override
  public void close() {
    super.close();

    logger.info("Closing OffsetAwareResilientEventHubReceiver");

    if(offsetDumper != null && lastMessage != null){
      EventHubMessage ehMessage = EventHubMessage.parseAmqpMessage(lastMessage);

      try {
        offsetDumper.apply(ehMessage.getOffset(), partitionId, consumerGroupName);
      } catch (Exception e){
        logger.error(String.format("Unable to persist offset '%s' for partitionId '%s' and consumerGroup '%s'", ehMessage.getOffset(), partitionId, consumerGroupName),e);

        throw new RuntimeException(e);
      }
    }
  }
}
