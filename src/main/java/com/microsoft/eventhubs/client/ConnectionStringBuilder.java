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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Build an amqps connection string that can be used to create EventHubClient
 */
public class ConnectionStringBuilder {
  private String connectionString;
  
  public ConnectionStringBuilder(String policyName, String policyKey, String namespace) {
    this(policyName, policyKey, namespace, Constants.ServiceFqdnSuffix);
  }

  public ConnectionStringBuilder(String policyName, String policyKey,
      String namespace, String serviceFqdnSuffix) {
    connectionString = "amqps://" + policyName + ":" + encodeString(policyKey)
        + "@" + namespace + "." + serviceFqdnSuffix;
  }
  
  private static String encodeString(String input) {
    try {
      return URLEncoder.encode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      //We don't need to throw this exception because the exception won't
      //happen because of user input. Our unit tests will catch this error.
      return "";
    }
  }
  
  public String getConnectionString() {
    return connectionString;
  }
}
