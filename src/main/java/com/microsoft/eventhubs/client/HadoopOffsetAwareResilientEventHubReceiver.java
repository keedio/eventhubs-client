package com.microsoft.eventhubs.client;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;

/**
 * Uses Hadoop classes to store offset checkpoint dir to an hadoop-compatible file system.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 13/1/16.
 */
public class HadoopOffsetAwareResilientEventHubReceiver extends OffsetAwareResilientEventHubReceiver {
  private static final Logger logger = LoggerFactory.getLogger(HadoopOffsetAwareResilientEventHubReceiver.class);
  static FileSystem hadoopFS;

  static {
    try {
      hadoopFS = FileSystem.get(new Configuration());
    } catch (IOException e) {
      logger.error("Unable to instantiate hadoop filesystem object");
    }
  }

  public HadoopOffsetAwareResilientEventHubReceiver(
          String connectionString,
          String eventHubName,
          String partitionId,
          String consumerGroupName,
          int defaultCredits,
          String baseHadoopDumpPath) throws IOException {

    super(connectionString,
            eventHubName,
            partitionId,
            consumerGroupName,
            defaultCredits,
            (_partitionId, _consumerGroup) -> {

              String fullHadoopPath = baseHadoopDumpPath + Path.SEPARATOR + _partitionId + Path.SEPARATOR + _consumerGroup;

              try {
                Path path = new Path(fullHadoopPath);

                try (ObjectInputStream is = new ObjectInputStream(hadoopFS.open(path))) {
                  ImmutableTriple<String, String, String> record = (ImmutableTriple<String, String, String>) is.readObject();
                  return Optional.of(record.getRight());
                }

              } catch (IOException | ClassNotFoundException e) {
                logger.error("Exception in offset resolver function: " + e.getLocalizedMessage());
              }
              return Optional.empty();
            },

            (_offset, _partitionId, _consumerGroup) -> {
              String fullHadoopPath = baseHadoopDumpPath + Path.SEPARATOR + _partitionId + Path.SEPARATOR + _consumerGroup;

              logger.info("Dumping last offset to: "+ fullHadoopPath);

              Path path = new Path(fullHadoopPath);

              Triple<String, String, String> record = new ImmutableTriple<>(_partitionId, _consumerGroup, _offset);
              try (ObjectOutputStream os = new ObjectOutputStream(hadoopFS.create(path, true))) {
                os.writeObject(record);
              }
            });
  }
}
