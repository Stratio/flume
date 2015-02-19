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
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_HOSTS;
import static org.apache.flume.api.RpcClientConfigurationConstants.CONFIG_HOST_SELECTOR;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyListOf;
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
import org.apache.flume.api.RpcTestUtils.LoadBalancedAvroHandler;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.util.OrderSelector;
import org.fest.assertions.Delta;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import junit.framework.Assert;

@PrepareForTest({LoadBalancingRpcClient.class, RpcClientFactory.class, OrderSelector.class})
public class TestLoadBalancingRpcClient {

  @Rule
  public PowerMockRule powerMock = new PowerMockRule();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  LoadBalancingRpcClient c;
  LoadBalancingRpcClient c2;
  RpcClientAnswer rpcClientAnswer;

  @Before
  public void setUp() {
    PowerMockito.mockStatic(RpcClientFactory.class, withSettings().stubOnly());
    currentTimeMillis = 0;
    PowerMockito.spy(System.class);
    PowerMockito.when(System.currentTimeMillis()).then(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
        return currentTimeMillis;
      }
    });
  }

  private long currentTimeMillis;

  private void setCurrentTimeMillis(final long currentTimeMillis) {
    this.currentTimeMillis = currentTimeMillis;
  }

  @After
  public void tearDown() {
    if (c != null) {
      c.close();
      c = null;
    }
    if (c2 != null) {
      c2.close();
      c2 = null;
    }
    rpcClientAnswer = null;
  }

  @Test
  public void testCreatingLbClientSingleHostFails() {
    Properties p = new Properties();
    p.put("host1", "127.0.0.1:80");
    p.put("hosts", "host1");
    p.put("client.type", "default_loadbalance");
    c = new LoadBalancingRpcClient();
    thrown.expect(FlumeException.class);
    thrown.expectMessage("At least two hosts are required to use the load balancing RPC client.");
    c.configure(p);
  }

  @Test
  public void testTwoHostFailover() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    Assert.assertTrue(c instanceof LoadBalancingRpcClient);

    for (int i = 0; i < 100; i++) {
      if (i == 20) {
        assertThat(rpcClientAnswer.getClients().size()).isEqualTo(2);
        mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
      } else if (i == 40) {
        mockRpcClientStatus(rpcClientAnswer.getClients().get(1), true);
      }
      c.append(getEvent(i));
    }

    verify(rpcClientAnswer.getClients().get(0), times(60)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(50)).append(any(Event.class));
  }

  @Test
  // This will fail without FLUME-1823
  public void testTwoHostFailoverThrowAfterClose() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < 5; i++) {
      c.append(getEvent(i));
    }

    c.close();
    thrown.expect(EventDeliveryException.class);
    thrown.expectMessage("Rpc Client is closed");
    c.append(getEvent(3));
  }

  /**
   * Ensure that we can tolerate a host that is completely down.
   * @throws Exception
   */
  @Test
  public void testTwoHostsOneDead() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);

    c = new LoadBalancingRpcClient();
    c.configure(p);
    for (int i = 0; i < 10; i++) {
      c.append(getEvent(i));
    }
    c.close();

    verify(rpcClientAnswer.getClients().get(0), times(10)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(5)).append(any(Event.class));
  }

  @Test
  public void testTwoHostsOneDeadBatch() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);

    c = new LoadBalancingRpcClient();
    c.configure(p);
    for (int i = 0; i < 10; i++) {
      c.appendBatch(getBatchedEvent(i));
    }
    c.close();

    verify(rpcClientAnswer.getClients().get(0), times(10)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(5)).appendBatch(anyListOf(Event.class));
  }

  @Test
  public void testTwoHostFailoverBatch() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < 100; i++) {
      if (i == 20) {
        assertThat(rpcClientAnswer.getClients().size()).isEqualTo(2);
        mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
      } else if (i == 40) {
        mockRpcClientStatus(rpcClientAnswer.getClients().get(1), true);
      }
      c.appendBatch(getBatchedEvent(i));
    }

    verify(rpcClientAnswer.getClients().get(0), times(60)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(50)).appendBatch(anyListOf(Event.class));
  }

  @Test
  public void testLbDefaultClientTwoHosts() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < 100; i++) {
      c.append(getEvent(i));
    }

    verify(rpcClientAnswer.getClients().get(0), times(50)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(50)).append(any(Event.class));
  }

  @Test
  public void testLbDefaultClientTwoHostsBatch() throws Exception {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < 100; i++) {
      c.appendBatch(getBatchedEvent(i));
    }

    verify(rpcClientAnswer.getClients().get(0), times(50)).appendBatch(anyListOf(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(50)).appendBatch(anyListOf(Event.class));
  }

  @Test
  public void testLbClientTenHostRandomDistribution() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i<NUM_HOSTS; i++) {
      h[i] = new LoadBalancedAvroHandler();
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:10" + i);
      hostList.append(name).append(" ");
    }

    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "random");

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < NUM_EVENTS; i++) {
      c.append(getEvent(i));
    }

    int total = 0;
    double[] probs = new double[NUM_HOSTS];
    double expectedProb = 1.0 / NUM_HOSTS;
    double allowedDeviation = 0.3;
    for (int i = 0; i < NUM_HOSTS; i++) {
      int calls = rpcClientAnswer.getClients().get(i).appendCalls;
      total += calls;
      probs[i] = calls;
      probs[i] /= NUM_EVENTS;
      assertThat(probs[i]).isEqualTo(expectedProb, Delta.delta(expectedProb * allowedDeviation));
    }
    assertThat(total).isEqualTo(NUM_EVENTS);
  }

  @Test
  public void testLbClientTenHostRandomDistributionBatch() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i<NUM_HOSTS; i++) {
      h[i] = new LoadBalancedAvroHandler();
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:10" + i);
      hostList.append(name).append(" ");
    }

    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "random");

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < NUM_EVENTS; i++) {
      c.appendBatch(getBatchedEvent(i));
    }

    int total = 0;
    double[] probs = new double[NUM_HOSTS];
    double expectedProb = 1.0 / NUM_HOSTS;
    double allowedDeviation = 0.3;
    for (int i = 0; i < NUM_HOSTS; i++) {
      int calls = rpcClientAnswer.getClients().get(i).appendBatchCalls;
      total += calls;
      probs[i] = calls;
      probs[i] /= NUM_EVENTS;
      assertThat(probs[i]).isEqualTo(expectedProb, Delta.delta(expectedProb * allowedDeviation));
    }
    assertThat(total).isEqualTo(NUM_EVENTS);
  }

  @Test
  public void testLbClientTenHostRoundRobinDistribution() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i<NUM_HOSTS; i++) {
      h[i] = new LoadBalancedAvroHandler();
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:10" + i);
      hostList.append(name).append(" ");
    }

    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < NUM_EVENTS; i++) {
      c.append(getEvent(i));
    }

    int total = 0;
    double[] probs = new double[NUM_HOSTS];
    double expectedProb = 1.0 / NUM_HOSTS;
    double allowedDeviation = 0.0;
    for (int i = 0; i < NUM_HOSTS; i++) {
      int calls = rpcClientAnswer.getClients().get(i).appendCalls;
      total += calls;
      probs[i] = calls;
      probs[i] /= NUM_EVENTS;
      assertThat(probs[i]).isEqualTo(expectedProb, Delta.delta(expectedProb * allowedDeviation));
    }
    assertThat(total).isEqualTo(NUM_EVENTS);
  }

  @Test
  public void testLbClientTenHostRoundRobinDistributionBatch() throws Exception {
    final int NUM_HOSTS = 10;
    final int NUM_EVENTS = 1000;
    LoadBalancedAvroHandler[] h = new LoadBalancedAvroHandler[NUM_HOSTS];
    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for (int i = 0; i<NUM_HOSTS; i++) {
      h[i] = new LoadBalancedAvroHandler();
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:10" + i);
      hostList.append(name).append(" ");
    }

    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < NUM_EVENTS; i++) {
      c.appendBatch(getBatchedEvent(i));
    }

    int total = 0;
    double[] probs = new double[NUM_HOSTS];
    double expectedProb = 1.0 / NUM_HOSTS;
    double allowedDeviation = 0.0;
    for (int i = 0; i < NUM_HOSTS; i++) {
      int calls = rpcClientAnswer.getClients().get(i).appendBatchCalls;
      total += calls;
      probs[i] = calls;
      probs[i] /= NUM_EVENTS;
      assertThat(probs[i]).isEqualTo(expectedProb, Delta.delta(expectedProb * allowedDeviation));
    }
    assertThat(total).isEqualTo(NUM_EVENTS);
  }

  @Test
  public void testRandomBackoff() throws Exception {
    final int NUM_HOSTS = 3;
    final int NUM_EVENTS = 50;
    final long MAX_BACKOFF = 1000;

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for(int i = 0; i < NUM_HOSTS;i++){
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:100" + i);
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "random");
    p.put("backoff", "true");
    p.put("maxBackoff", Long.toString(MAX_BACKOFF));

    // Hosts 0 and 2 should backoff
    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), false);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(2), false);

    setCurrentTimeMillis(0);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    // TODO: there is a remote possibility that s0 or s2
    // never get hit by the random assignment
    // and thus not backoffed, causing the test to fail
    for(int i=0; i < NUM_EVENTS; i++) {
      // a well behaved runner would always check the return.
      c.append(EventBuilder.withBody(("test" + String.valueOf(i)).getBytes()));
    }
    verify(rpcClientAnswer.getClients().get(0), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(50)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1)).append(any(Event.class));

    // Host 2 keeps backoff
    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), true);
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);

    try {
      c.append(EventBuilder.withBody("shouldfail".getBytes()));
      // nothing should be able to process right now
      Assert.fail("Expected EventDeliveryException");
    } catch (EventDeliveryException e) {
      // this is expected
    }

    setCurrentTimeMillis(1001); // wait for s0 to no longer be backed off

    for (int i = 0; i < NUM_EVENTS; i++) {
      // a well behaved runner would always check the return.
      c.append(EventBuilder.withBody(("test" + String.valueOf(i)).getBytes()));
    }
    verify(rpcClientAnswer.getClients().get(0), times(51)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(52)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(2)).append(any(Event.class));
  }

  @Test
  public void testRoundRobinBackoffInitialFailure() throws EventDeliveryException {
    final int NUM_HOSTS = 3;

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for(int i = 0; i < NUM_HOSTS;i++){
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:100" + i);
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");
    p.put("backoff", "true");

    c = new LoadBalancingRpcClient();
    c.configure(p);

    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    verify(rpcClientAnswer.getClients().get(0), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1)).append(any(Event.class));

    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    verify(rpcClientAnswer.getClients().get(0), times(1 + 2)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1 + 1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1 + 1)).append(any(Event.class));
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), true);

    //This time the iterators will never have "1".
    //So clients get in the order: 1 - 3 - 1
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("testing".getBytes()));
    }

    verify(rpcClientAnswer.getClients().get(0), times(1 + 2 + 1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1 + 1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1 + 1 + 2)).append(any(Event.class));
  }

  @Test
  public void testRoundRobinBackoffFailureRecovery() throws EventDeliveryException, InterruptedException {
    final int NUM_HOSTS = 3;
    final long MAX_BACKOFF = 1000;

    rpcClientAnswer = new RpcClientAnswer(NUM_HOSTS);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    Properties p = new Properties();
    StringBuilder hostList = new StringBuilder("");
    for(int i = 0; i < NUM_HOSTS;i++){
      String name = "h" + i;
      p.put("hosts." + name, "127.0.0.1:100" + i);
      hostList.append(name).append(" ");
    }
    p.put("hosts", hostList.toString().trim());
    p.put("client.type", "default_loadbalance");
    p.put("host-selector", "round_robin");
    p.put("backoff", "true");
    p.put("maxBackoff", Long.toString(MAX_BACKOFF));

    setCurrentTimeMillis(0);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
    for (int i = 0; i < 3; i++) {
      c.append(EventBuilder.withBody("recovery test".getBytes()));
    }
    verify(rpcClientAnswer.getClients().get(0), times(2)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1)).append(any(Event.class));
    mockRpcClientStatus(rpcClientAnswer.getClients().get(1), true);

    setCurrentTimeMillis(1001);
    int numEvents = 60;

    for(int i = 0; i < numEvents; i++){
      c.append(EventBuilder.withBody("testing".getBytes()));
    }
    verify(rpcClientAnswer.getClients().get(0), times(2 + numEvents / 3)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1 + numEvents / 3)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(2), times(1 + numEvents / 3)).append(any(Event.class));
  }

  /**
   * LoadBalancingRpcClient closes unactive clients (isActive() == false), but only
   * after second attempt.
   *
   * @throws EventDeliveryException
   */
  @Test
  public void testInactiveClientIsClosed() throws EventDeliveryException {
    Properties p = new Properties();
    p.put("hosts", "h1 h2");
    p.put("client.type", "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");

    rpcClientAnswer = new RpcClientAnswer(2);
    when(RpcClientFactory.getInstance(any(Properties.class))).then(rpcClientAnswer);

    mockRpcClientStatus(rpcClientAnswer.getClients().get(0), false);
    when(rpcClientAnswer.getClients().get(0).isActive()).thenReturn(false);

    c = new LoadBalancingRpcClient();
    c.configure(p);

    c.append(getEvent(1));
    verify(rpcClientAnswer.getClients().get(0), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(1)).append(any(Event.class));

    //mockRpcClientStatus(rpcClientAnswer.getClients().get(1), false);
    //when(rpcClientAnswer.getClients().get(1).isActive()).thenReturn(false);

    c.append(getEvent(1));
    verify(rpcClientAnswer.getClients().get(0), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(2)).append(any(Event.class));

    c.append(getEvent(1));
    verify(rpcClientAnswer.getClients().get(0), times(1)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(1), times(3)).append(any(Event.class));
    verify(rpcClientAnswer.getClients().get(0), times(1)).close();

    c.close();
  }

  @Test
  public void testBadHostSelector() throws EventDeliveryException {
    Properties p = new Properties();
    p.put(CONFIG_HOSTS, "h1 h2");
    p.put(CONFIG_CLIENT_TYPE, "default_loadbalance");
    p.put("hosts.h1", "127.0.0.1:80");
    p.put("hosts.h2", "127.0.0.1:81");
    p.put(CONFIG_HOST_SELECTOR, "org.apace.BadClassXXX");
    c = new LoadBalancingRpcClient();
    thrown.expect(FlumeException.class);
    thrown.expectMessage("Unable to instantiate host selector: org.apace.BadClassXXX");
    c.configure(p);
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
