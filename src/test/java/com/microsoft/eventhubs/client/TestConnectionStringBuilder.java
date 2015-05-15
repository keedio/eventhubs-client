package com.microsoft.eventhubs.client;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestConnectionStringBuilder {

  @Test
  public void testFqdnSuffix() {
    String connectString = new ConnectionStringBuilder("policy-name",
        "policyKey", "namespace", "mytest.com").getConnectionString();
    assertEquals("amqps://policy-name:policyKey@namespace.mytest.com", connectString);
  }

}
