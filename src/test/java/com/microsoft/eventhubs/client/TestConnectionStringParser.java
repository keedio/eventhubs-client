package com.microsoft.eventhubs.client;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestConnectionStringParser {

  @Test
  public void testPolicyKeyDecoding() throws Exception {
    String connectionString = "amqps://policyName:tBDm6DzznuXxtKK6P5wGW%2BMl9cVDw%2BN2NWSvvoLbMpE%3D@namespace.servicebus.windows.net/";
    ConnectionStringParser parser = new ConnectionStringParser(connectionString);
    assertEquals("namespace.servicebus.windows.net", parser.getHost());
    assertEquals(5671, parser.getPort());
    assertEquals(true, parser.getSsl());
    assertEquals("policyName", parser.getPolicyName());
    assertEquals("tBDm6DzznuXxtKK6P5wGW+Ml9cVDw+N2NWSvvoLbMpE=", parser.getPolicyKey());
  }

  @Test
  public void testInvalidStrings() throws Exception {
    boolean caught = false;
    
    try {
      ConnectionStringParser parser = new ConnectionStringParser("");
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      ConnectionStringParser parser = new ConnectionStringParser(null);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      //invalid protocol name
      String connectionString = "invalid://policy-name:policyKey@namespace.servicebus.windows.net/";
      ConnectionStringParser parser = new ConnectionStringParser(connectionString);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      //no policy key
      String connectionString = "amqps://policy-name@namespace.servicebus.windows.net/";
      ConnectionStringParser parser = new ConnectionStringParser(connectionString);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      //no policy name
      String connectionString = "amqps://:policyKey@namespace.servicebus.windows.net/";
      ConnectionStringParser parser = new ConnectionStringParser(connectionString);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
  }
  
  @Test
  public void testValidStrings() throws Exception {
    String connectionString = "amqps://policy-name:policyKey@namespace.servicebus.windows.net:9090/";
    ConnectionStringParser parser = new ConnectionStringParser(connectionString);
    assertEquals("namespace.servicebus.windows.net", parser.getHost());
    assertEquals(9090, parser.getPort());
    assertEquals(true, parser.getSsl());
    assertEquals("policy-name", parser.getPolicyName());
    assertEquals("policyKey", parser.getPolicyKey());
  }

  @Test
  public void testPolicyKeyEncoding() {
    //Policy Key is generated randomly by EventHubs server, it can contain
    //special characters like '+', '=', '/' etc.
    String connectString = new ConnectionStringBuilder("policyName",
        "tBDm6DzznuXxtKK6P5wGW+Ml9cVDw+N2NWSvvoLbMpE=", "namespace").getConnectionString();
    assertEquals("amqps://policyName:tBDm6DzznuXxtKK6P5wGW%2BMl9cVDw%2BN2NWSvvoLbMpE%3D@namespace.servicebus.windows.net", connectString);
  }
}
