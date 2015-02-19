package org.apache.flume.api;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class RpcClientAnswer implements Answer<RpcClient> {

  private int idx;
  private boolean loop;
  private List<MockRpcClient> clients;

  public RpcClientAnswer(int n) {
    this(n, false);
  }

  public RpcClientAnswer(int n, boolean loop) {
    this();
    for (int i = 0; i < n; i++) {
      clients.add(getMockRpcClient());
    }
    this.loop = loop;
  }

  public RpcClientAnswer(MockRpcClient...clients) {
    this.clients = new ArrayList<MockRpcClient>(Arrays.asList(clients));
  }

  public List<MockRpcClient> getClients() {
    return clients;
  }

  public static MockRpcClient getMockRpcClient() {
    return spy(new MockRpcClient());
  }

  public static void mockRpcClientStatus(final RpcClient rpcClient, final boolean success) {
    try {
      if (success) {
        doNothing().when(rpcClient).append(any(Event.class));
        doNothing().when(rpcClient).appendBatch(anyListOf(Event.class));
      } else {
        doThrow(EventDeliveryException.class).when(rpcClient).append(any(Event.class));
        doThrow(EventDeliveryException.class).when(rpcClient).appendBatch(anyListOf(Event.class));
      }
    } catch (EventDeliveryException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public RpcClient answer(InvocationOnMock invocation) throws Throwable {
    if (idx >= clients.size() && loop) {
      idx = 0;
      int n = clients.size();
      clients.clear();
      for (int i = 0; i < n; i++) {
        clients.add(getMockRpcClient());
      }
    }
    return clients.get(idx++);
  }

  public static class MockRpcClient implements RpcClient {

    int batchSize;
    int appendCalls;
    int appendBatchCalls;
    boolean isClosed;

    public void setBatchSize(final int batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
      return batchSize;
    }

    @Override
    public void append(Event event) throws EventDeliveryException {
      appendCalls++;
      if (isClosed) {
        throw new EventDeliveryException();
      }
    }

    @Override public void appendBatch(List<Event> events) throws EventDeliveryException {
      appendBatchCalls++;
      if (isClosed) {
        throw new EventDeliveryException();
      }
    }

    @Override public boolean isActive() {
      return !isClosed;
    }

    @Override public void close() throws FlumeException {
      isClosed = true;
    }
  }

}