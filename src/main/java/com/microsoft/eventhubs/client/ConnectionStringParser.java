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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLStreamHandler;

/**
 * Parse AMQP connection string and get policyName, policyKey, host etc.
 * The format of the connection string is:
 *   amqp[s]://{policyName}:{policyKey}@{host}/
 */
public class ConnectionStringParser {

  private final String connectionString;

  private String host;
  private int port;
  private String policyName;
  private String policyKey;
  private boolean ssl;

  public ConnectionStringParser(String connectionString) throws EventHubException {
    this.connectionString = connectionString;
    initialize();
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public String getPolicyName() {
    return this.policyName;
  }

  public String getPolicyKey() {
    return this.policyKey;
  }

  public boolean getSsl() {
    return this.ssl;
  }

  private void initialize() throws EventHubException {
    URL url;
    try {
      url = new URL(null, this.connectionString, new NullURLStreamHandler());
    } catch (MalformedURLException e) {
      throw new EventHubException("connectionString is not valid.", e);
    }

    String protocol = url.getProtocol();
    this.ssl = protocol.equalsIgnoreCase(Constants.SslScheme);
    this.host = url.getHost();
    this.port = url.getPort();

    if (this.port == -1) {
      this.port = this.ssl ? Constants.DefaultSslPort : Constants.DefaultPort;
    }

    String userInfo = url.getUserInfo();
    if (userInfo != null) {
      String[] credentials = userInfo.split(":", 2);
      if(credentials.length != 2) {
        throw new EventHubException("connectionString does not contain policy info.");
      }
      try {
        policyName = URLDecoder.decode(credentials[0], "UTF-8");
        policyKey = URLDecoder.decode(credentials[1], "UTF-8");
      }
      catch(UnsupportedEncodingException ex) {
        throw new EventHubException(ex);
      }
    }
  }

  class NullURLStreamHandler extends URLStreamHandler {
    protected URLConnection openConnection(URL u) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
