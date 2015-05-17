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
      new ConnectionStringParser("");
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      new ConnectionStringParser(null);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      //invalid protocol name
      String connectionString = "invalid://policy-name:policyKey@namespace.servicebus.windows.net/";
      new ConnectionStringParser(connectionString);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      //no policy key
      String connectionString = "amqps://policy-name@namespace.servicebus.windows.net/";
      new ConnectionStringParser(connectionString);
    }
    catch(EventHubException e) {
      caught = true;
    }
    assertTrue(caught);
    
    caught = false;
    try {
      //no policy name
      String connectionString = "amqps://:policyKey@namespace.servicebus.windows.net/";
      new ConnectionStringParser(connectionString);
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
