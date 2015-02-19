/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.api;

import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_BATCH_SIZE;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_HOSTS;
import static org.fest.assertions.Assertions.assertThat;
import static org.fest.reflect.core.Reflection.field;

import java.io.File;
import java.util.Properties;

import org.apache.avro.ipc.Server;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcTestUtils.OKAvroHandler;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestRpcClientFactory {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  private static final String localhost = "localhost";

  private RpcClient client;
  private Server server;
  private ThriftTestingSource thriftSource;

  int FREE_PORT = 51923;

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
      client = null;
    }
    if (server != null) {
      server.close();
      server = null;
    }
    if (thriftSource != null) {
      thriftSource.stop();
      thriftSource = null;
    }
  }

  @Test
  public void testGetDefaultInstanceHostPort() throws FlumeException,
      EventDeliveryException {
    server = RpcTestUtils.startServer(new OKAvroHandler());
    client = RpcClientFactory.getDefaultInstance(localhost, server.getPort());
    assertThat(client).isInstanceOf(NettyAvroRpcClient.class);
    NettyAvroRpcClient avroClient = (NettyAvroRpcClient)client;
    assertThat(avroClient.getBatchSize()).isEqualTo(RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE);
    assertThat(avroClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(avroClient.requestTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(avroClient.isActive()).isTrue();
  }

  @Test
  public void testGetDefaultInstanceHostPortBadHost() throws FlumeException {
    thrown.expect(FlumeException.class);
    thrown.expectMessage("RPC connection error");
    client = RpcClientFactory.getDefaultInstance("BAD_HOST_XXX", 1024);
  }

  @Test
  public void testGetDefaultInstanceHostPortBatch() throws FlumeException,
      EventDeliveryException {
    int BATCH_SIZE = 200;
    server = RpcTestUtils.startServer(new OKAvroHandler());
    client = RpcClientFactory.getDefaultInstance(localhost, server.getPort(), BATCH_SIZE);
    assertThat(client).isInstanceOf(NettyAvroRpcClient.class);
    NettyAvroRpcClient avroClient = (NettyAvroRpcClient)client;
    assertThat(avroClient.getBatchSize()).isEqualTo(BATCH_SIZE);
    assertThat(avroClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(avroClient.requestTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(avroClient.isActive()).isTrue();
  }

  @Test
  public void testGetDefaultInstanceHostPortBatchBadHost() throws FlumeException {
    thrown.expect(FlumeException.class);
    thrown.expectMessage("RPC connection error");
    int BATCH_SIZE = 200;
    client = RpcClientFactory.getDefaultInstance("BAD_HOST_XXX", 1024, BATCH_SIZE);
  }

  @Test
  public void testGetDefaultInstanceHostPortNullHost() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("hostname must not be null");
    client = RpcClientFactory.getDefaultInstance(null, 1024);
  }

  @Test
  public void testGetDefaultInstanceHostPortNullPort() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("port must not be null");
    client = RpcClientFactory.getDefaultInstance("hostname", null);
  }

  @Test
  public void testGetDefaultInstanceHostPortBatchNullBatch() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("batchSize must not be null");
    client = RpcClientFactory.getDefaultInstance("hostname", 1024, null);
  }

  @Test
  public void testGetInstanceHostPort() throws FlumeException,
      EventDeliveryException {
    server = RpcTestUtils.startServer(new OKAvroHandler());
    client = RpcClientFactory.getInstance(localhost, server.getPort());
    assertThat(client).isInstanceOf(NettyAvroRpcClient.class);
    NettyAvroRpcClient avroClient = (NettyAvroRpcClient)client;
    assertThat(avroClient.getBatchSize()).isEqualTo(RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE);
    assertThat(avroClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(avroClient.requestTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(avroClient.isActive()).isTrue();
  }

  @Test
  public void testGetInstanceHostPortBadHost() throws FlumeException {
    thrown.expect(FlumeException.class);
    thrown.expectMessage("RPC connection error");
    client = RpcClientFactory.getInstance("BAD_HOST_XXX", 1024);
  }

  @Test
  public void testGetInstanceHostPortBatch() throws FlumeException,
      EventDeliveryException {
    int BATCH_SIZE = 200;
    server = RpcTestUtils.startServer(new OKAvroHandler());
    client = RpcClientFactory.getInstance(localhost, server.getPort(), BATCH_SIZE);
    assertThat(client).isInstanceOf(NettyAvroRpcClient.class);
    NettyAvroRpcClient avroClient = (NettyAvroRpcClient)client;
    assertThat(avroClient.getBatchSize()).isEqualTo(BATCH_SIZE);
    assertThat(avroClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(avroClient.requestTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(avroClient.isActive()).isTrue();
  }

  @Test
  public void testGetInstanceHostPortBatchBadHost() throws FlumeException {
    thrown.expect(FlumeException.class);
    thrown.expectMessage("RPC connection error");
    int BATCH_SIZE = 200;
    client = RpcClientFactory.getInstance("BAD_HOST_XXX", 1024, BATCH_SIZE);
  }

  @Test
  public void testGetInstanceHostPortNullHost() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("hostname must not be null");
    client = RpcClientFactory.getInstance(null, 1024);
  }

  @Test
  public void testGetInstanceHostPortNullPort() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("port must not be null");
    client = RpcClientFactory.getInstance("hostname", null);
  }

  @Test
  public void testGetInstanceHostPortBatchNullBatch() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("batchSize must not be null");
    client = RpcClientFactory.getInstance("hostname", 1024, null);
  }

  @Test
  public void testGetInstancePropertiesHost() throws FlumeException,
      EventDeliveryException {
    server = RpcTestUtils.startServer(new OKAvroHandler());
    Properties p = new Properties();
    p.put(CONFIG_HOSTS, "host1");
    p.put("hosts.host1", localhost + ":" + String.valueOf(server.getPort()));
    client = RpcClientFactory.getInstance(p);
    assertThat(client).isInstanceOf(NettyAvroRpcClient.class);
    NettyAvroRpcClient avroClient = (NettyAvroRpcClient)client;
    assertThat(avroClient.getBatchSize()).isEqualTo(RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE);
    assertThat(avroClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(avroClient.requestTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(avroClient.isActive()).isTrue();
  }

  @Test
  public void testGetInstancePropertiesBadHost() throws FlumeException {
    int BATCH_SIZE = 200;
    Properties p = new Properties();
    p.put(CONFIG_HOSTS, "host1");
    p.put("hosts.host1", "BAD_HOST_XXX:1024");
    p.put(CONFIG_BATCH_SIZE, Integer.toString(BATCH_SIZE));
    thrown.expect(FlumeException.class);
    thrown.expectMessage("RPC connection error");
    client = RpcClientFactory.getInstance(p);
  }

  @Test
  public void testGetInstancePropertiesHostBatch() throws FlumeException,
      EventDeliveryException {
    int BATCH_SIZE = 200;
    server = RpcTestUtils.startServer(new OKAvroHandler());
    Properties p = new Properties();
    p.put(CONFIG_HOSTS, "host1");
    p.put("hosts.host1", localhost + ":" + String.valueOf(server.getPort()));
    p.put("batch-size", Integer.toString(BATCH_SIZE));
    client = RpcClientFactory.getInstance(p);
    assertThat(client).isInstanceOf(NettyAvroRpcClient.class);
    NettyAvroRpcClient avroClient = (NettyAvroRpcClient)client;
    assertThat(avroClient.getBatchSize()).isEqualTo(BATCH_SIZE);
    assertThat(avroClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(avroClient.requestTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(avroClient.isActive()).isTrue();
  }

  @Test
  public void testGetInstanceCustomBadClass() throws FlumeException,
      EventDeliveryException {
    Properties p = new Properties();
    p.put(CONFIG_CLIENT_TYPE, "org.apache.BadClassXXX");
    thrown.expect(FlumeException.class);
    thrown.expectMessage("No such client!");
    client = RpcClientFactory.getInstance(p);
  }

  @Test
  public void testGetInstancePropertiesNull() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("properties must not be null");
    client = RpcClientFactory.getInstance((Properties)null);
  }

  @Test
  public void testGetInstancePropertiesFileNull() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("propertiesFile must not be null");
    client = RpcClientFactory.getInstance((File)null);
  }

  @Test
  public void testGetThriftInstanceHostPort() throws Exception {
    thriftSource = new ThriftTestingSource(ThriftTestingSource.HandlerType.FAIL.name(), FREE_PORT, ThriftRpcClient.COMPACT_PROTOCOL);
    client = RpcClientFactory.getThriftInstance(localhost, FREE_PORT);
    assertThat(client).isInstanceOf(ThriftRpcClient.class);
    ThriftRpcClient thriftClient = (ThriftRpcClient)client;
    assertThat(thriftClient.getBatchSize()).isEqualTo(RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE);
    assertThat(thriftClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(field("requestTimeout").ofType(long.class).in(thriftClient).get()).isEqualTo(
        RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(thriftClient.isActive()).isTrue();
  }

  // https://issues.apache.org/jira/browse/FLUME-2627
  @Test
  public void testGetThriftInstanceHostPortBadHost() throws FlumeException {
    client = RpcClientFactory.getThriftInstance("BAD_HOST_XXX", 1024);
    assertThat(client).isInstanceOf(ThriftRpcClient.class);
    ThriftRpcClient thriftClient = (ThriftRpcClient)client;
    assertThat(thriftClient.isActive()).isEqualTo(true);
  }

  @Test
  public void testGetThriftInstanceHostPortBatch() throws Exception {
    int BATCH_SIZE = 200;
    thriftSource = new ThriftTestingSource(ThriftTestingSource.HandlerType.FAIL.name(), FREE_PORT, ThriftRpcClient.COMPACT_PROTOCOL);
    client = RpcClientFactory.getThriftInstance(localhost, FREE_PORT, BATCH_SIZE);
    assertThat(client).isInstanceOf(ThriftRpcClient.class);
    ThriftRpcClient thriftClient = (ThriftRpcClient)client;
    assertThat(thriftClient.getBatchSize()).isEqualTo(BATCH_SIZE);
    assertThat(thriftClient.connectTimeout).isEqualTo(RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS);
    assertThat(field("requestTimeout").ofType(long.class).in(thriftClient).get()).isEqualTo(
        RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS);
    assertThat(thriftClient.isActive()).isTrue();
  }

  // https://issues.apache.org/jira/browse/FLUME-2627
  @Test
  public void testGetThriftInstanceHostPortBatchBadHost() throws FlumeException {
    int BATCH_SIZE = 200;
    client = RpcClientFactory.getThriftInstance("BAD_HOST_XXX", 1024, BATCH_SIZE);
    assertThat(client).isInstanceOf(ThriftRpcClient.class);
    ThriftRpcClient thriftClient = (ThriftRpcClient)client;
    assertThat(thriftClient.isActive()).isEqualTo(true);
  }

  @Test
  public void testGetThriftInstancePropertiesEmpty() throws FlumeException {
    Properties p = new Properties();
    thrown.expect(FlumeException.class);
    thrown.expectMessage("No host specified");
    client = RpcClientFactory.getThriftInstance(p);
  }

  @Test
  public void testGetThriftInstanceProperties() throws Exception {
    thriftSource = new ThriftTestingSource(ThriftTestingSource.HandlerType.FAIL.name(), FREE_PORT, ThriftRpcClient.COMPACT_PROTOCOL);
    Properties p = new Properties();
    p.put(CONFIG_HOSTS, "host1");
    p.put("hosts.host1", localhost + ":" + FREE_PORT);
    client = RpcClientFactory.getThriftInstance(p);
    assertThat(client).isInstanceOf(ThriftRpcClient.class);
    ThriftRpcClient thriftClient = (ThriftRpcClient)client;
    assertThat(thriftClient.isActive()).isEqualTo(true);
  }

  @Test
  public void testGetThriftInstanceHostPortNullHost() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("hostname must not be null");
    client = RpcClientFactory.getThriftInstance(null, 1024);
  }

  @Test
  public void testGetThriftInstanceHostPortNullPort() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("port must not be null");
    client = RpcClientFactory.getThriftInstance("hostname", null);
  }

  @Test
  public void testGetThriftInstanceHostPortBatchNullBatch() throws FlumeException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("batchSize must not be null");
    client = RpcClientFactory.getThriftInstance("hostname", 1024, null);
  }

}
