package com.microsoft.eventhubs.client;

import java.io.IOException;

/**
 * Defines a generic function used to persist the last received offset to an external datastore.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 12/1/16.
 */
@FunctionalInterface
public interface OffsetDumperFunction {
  void apply(String offset, String partitionId, String consumerGroup) throws IOException;
}
