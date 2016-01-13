package com.microsoft.eventhubs.client;

import com.google.common.io.Files;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.qpid.amqp_1_0.client.Message;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import static org.junit.Assert.*;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 13/1/16.
 */
public class TestHadoopOffsetAwareResilientEventHubReceiver extends AbstractOffsetAwareEventHubReceiverTest {

  class TestableHadoopOffsetAwareResilientEventHubReceiver extends HadoopOffsetAwareResilientEventHubReceiver {
    public TestableHadoopOffsetAwareResilientEventHubReceiver(String connectionString,
                                                        String eventHubName, String partitionId, String consumerGroupName,
                                                        int defaultCredits, String hadoopPath,
                                                        Message fakeMessage) throws IOException {
      super(connectionString, eventHubName, partitionId, consumerGroupName,
              defaultCredits, hadoopPath);
      lastMessage = fakeMessage;
    }
  }

  @Test
  public void testOffsetRetrievalEmptyPath() throws IOException {
    File f = Files.createTempDir();
    String path = f.getAbsolutePath();

    HadoopOffsetAwareResilientEventHubReceiver receiver = new HadoopOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1, path);

    File hadoopPath = new File(f, "0" + File.pathSeparator +  "myConsumerGroupName");
    assertFalse(hadoopPath.exists());
    assertNull(receiver.lastMessage);
  }

  @Test
   public void testOffsetDump() throws IOException, ClassNotFoundException {
    File f = Files.createTempDir();
    String path = f.getAbsolutePath();

    Message fakeMessage = fakeOffsetWrapperMessage("338422478789101");

    TestableHadoopOffsetAwareResilientEventHubReceiver receiver = new TestableHadoopOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1, path, fakeMessage);

    File hadoopPath = new File(f, "0" + File.separator +  "myConsumerGroupName");
    assertFalse(hadoopPath.exists());
    assertNotNull(receiver.lastMessage);

    receiver.close();

    assertTrue(hadoopPath.exists());

    try (ObjectInputStream is = new ObjectInputStream(new FileInputStream(hadoopPath))) {
      ImmutableTriple<String, String, String> record = (ImmutableTriple<String, String, String>) is.readObject();
      assertEquals("338422478789101", record.getRight());
      assertEquals("myConsumerGroupName", record.getMiddle());
      assertEquals("0", record.getLeft());
    }
  }

  @Test
  public void testOffsetRetrieval() throws IOException, ClassNotFoundException {
    File f = Files.createTempDir();
    String path = f.getAbsolutePath();

    Message fakeMessage = fakeOffsetWrapperMessage("338422478789101");

    TestableHadoopOffsetAwareResilientEventHubReceiver receiver = new TestableHadoopOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1, path, fakeMessage);

    File hadoopPath = new File(f, "0" + File.separator +  "myConsumerGroupName");
    assertFalse(hadoopPath.exists());
    assertNotNull(receiver.lastMessage);

    receiver.close();

    HadoopOffsetAwareResilientEventHubReceiver receiver1 = new HadoopOffsetAwareResilientEventHubReceiver(
            "connectionString", "name", "0", "myConsumerGroupName", -1, path);

    assertNotNull(receiver1.filter);
    assertEquals("338422478789101", receiver1.filter.getFilterValue());
  }
}
