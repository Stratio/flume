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

import static org.apache.flume.api.RpcClientAnswer.mockRpcClientStatus;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyListOf;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

@PrepareForTest({FailoverRpcClient.class, RpcClientFactory.class})
public class TestFailoverRpcClient {

  @Rule
  public PowerMockRule powerMock = new PowerMockRule();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  FailoverRpcClient c;
  RpcClientAnswer rpcClientAnswer;

  @Before
  public void setUp() {
    PowerMockito.mockStatic(RpcClientFactory.class, withSettings().stubOnly());
  }

  @After
  public void tearDown() {
    if (c != null) {
      c.close();
      c = null;
    }
    rpcClientAnswer = null;
  }

  /**
   * Test a bunch of servers closing the one we are writing to and bringing
   * another one back online.
   *
   * @throws FlumeException
   * @throws EventDeliveryException
   * @throws InterruptedException
   */
  @Test
  public void testFailover() throws FlumeException, EventDeliveryException, InterruptedException {

    Properties props = new Properties();
    props.put("client.type", "default_failover");
    props.put("hosts", "host1 host2 host3");
    props.put("hosts.host1", "127.0.0.1:81");
    props.put("hosts.host2", "127.0.0.1:82");
    props.put("hosts.host3", "127.0.0.1:83");

    rpcClientAnswer = new RpcClientAnswer(3, true);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    FailoverRpcClient client = new FailoverRpcClient();
    client.configure(props);

    // Active: 0
    client.appendBatch(getBatchedEvent(1));
    verify(rpcClientAnswer.getClients().get(0), times(1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), never()).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), never()).appendBatch(anyListOf(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), false);

    // Active: 0 -> fails -> active: 1
    client.appendBatch(getBatchedEvent(2));
    verify(rpcClientAnswer.getClients().get(0), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), never()).appendBatch(anyListOf(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);

    // Active: 1 -> fails -> active: 2
    client.appendBatch(getBatchedEvent(3));
    verify(rpcClientAnswer.getClients().get(0), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1)).appendBatch(anyListOf(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), true);

    // Active: 2 -> succeeds
    client.appendBatch(getBatchedEvent(4));
    verify(rpcClientAnswer.getClients().get(0), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1 + 1)).appendBatch(anyListOf(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), true);

    // Active: 2 -> succeeds
    client.appendBatch(getBatchedEvent(5));
    verify(rpcClientAnswer.getClients().get(0), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1 + 1 + 1)).appendBatch(anyListOf(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(2), false);

    // Active: 2 -> fails -> active: 0
    // RPC clients are closed and instantiated again; so we start counting again
    client.appendBatch(getBatchedEvent(6));
    verify(rpcClientAnswer.getClients().get(0), times(1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), never()).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), never()).appendBatch(anyListOf(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), false);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(2), true);

    // Active: 0 -> fails -> active: 1 -> fails -> active: 2
    client.appendBatch(getBatchedEvent(7));
    verify(rpcClientAnswer.getClients().get(0), times(1 + 1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1)).appendBatch(anyListOf(Event.class));
  }

  /**
   * Try writing to some servers and then kill them all.
   *
   * @throws FlumeException
   * @throws EventDeliveryException
   */
  @Test
  public void testAllFailedServers() throws FlumeException, EventDeliveryException {
    Properties props = new Properties();
    props.put("client.type", "default_failover");
    props.put("hosts", "host1 host2 host3");
    props.put("hosts.host1", "127.0.0.1:81");
    props.put("hosts.host2", "127.0.0.1:82");
    props.put("hosts.host3", "127.0.0.1:83");

    rpcClientAnswer = new RpcClientAnswer(3, true);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    FailoverRpcClient client = new FailoverRpcClient();
    client.configure(props);

    client.appendBatch(getBatchedEvent(1));
    client.appendBatch(getBatchedEvent(2));
    client.appendBatch(getBatchedEvent(3));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), false);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(2), false);

    thrown.expect(EventDeliveryException.class);
    thrown.expectMessage("Failed to send the event!");
    client.appendBatch(getBatchedEvent(1));
  }

  private List<Event> getBatchedEvent(int index) {
    List<Event> result = new ArrayList<Event>();
    result.add(getEvent(index));
    return result;
  }

  private Event getEvent(int index) {
    return EventBuilder.withBody(("event: " + index).getBytes());
  }


}
