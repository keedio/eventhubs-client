package com.microsoft.eventhubs.client;

import java.util.Optional;

/**
 * Defines a generic function used to resolve the last read offset from an external data store.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 13/1/16.
 */
@FunctionalInterface
public interface OffsetResolverFunction {
  Optional<String> apply(String partitionId, String consumerGroup);
}
