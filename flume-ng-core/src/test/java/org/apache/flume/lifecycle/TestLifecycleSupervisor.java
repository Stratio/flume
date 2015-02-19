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

package org.apache.flume.lifecycle;

import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestLifecycleSupervisor {

  private LifecycleSupervisor supervisor;

  @Before
  public void setUp() {
    supervisor = new LifecycleSupervisor();
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {
    supervisor.start();
    supervisor.stop();
  }

  //TODO: What is actually being tested here?
  @Test
  public void testSupervise() throws LifecycleException, InterruptedException {
    supervisor.start();

    /* Attempt to supervise a known-to-fail config. */
    /*
     * LogicalNode node = new LogicalNode(); SupervisorPolicy policy = new
     * SupervisorPolicy.OnceOnlyPolicy(); supervisor.supervise(node, policy,
     * LifecycleState.START);
     */

    final MockLifecycleAware node1 = spy(new MockLifecycleAware());

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node1, policy, LifecycleState.START);

    verify(node1, timeout(10000)).start();

    final MockLifecycleAware node2 = spy(new MockLifecycleAware());

    policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node2, policy, LifecycleState.START);

    verify(node2, timeout(5000)).start();

    supervisor.stop();
  }

  @Test
  public void testSuperviseBroken() throws LifecycleException,
      InterruptedException {

    supervisor.start();

    /* Attempt to supervise a known-to-fail config. */
    LifecycleAware node = spy(new LifecycleAware() {

      @Override
      public void stop() {
      }

      @Override
      public void start() {
        throw new NullPointerException("Boom!");
      }

      @Override
      public LifecycleState getLifecycleState() {
        return LifecycleState.IDLE;
      }
    });

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    verify(node, timeout(5000)).start();
    verify(node, timeout(5000).atLeast(1)).getLifecycleState();

    supervisor.stop();

    verify(node, timeout(5000).atLeast(2)).getLifecycleState();
    verify(node, never()).stop();
    verifyNoMoreInteractions(node);
  }

  @Test
  public void testSuperviseSupervisor() throws LifecycleException,
      InterruptedException {

    supervisor.start();

    LifecycleSupervisor supervisor2 = spy(new LifecycleSupervisor());

    MockLifecycleAware node = spy(new MockLifecycleAware());

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor2.supervise(node, policy, LifecycleState.START);

    supervisor.supervise(supervisor2,
        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);

    verify(supervisor2, timeout(10000).times(1)).start();
    verify(node, timeout(10000).times(1)).start();

    supervisor.stop();

    verify(supervisor2, timeout(10000).times(1)).stop();
    verify(node, timeout(10000).times(1)).stop();
    verify(supervisor2, atLeast(1)).getLifecycleState();
    verify(node, atLeast(1)).getLifecycleState();
  }

  @Test
  public void testUnsuperviseServce() throws LifecycleException,
      InterruptedException {

    supervisor.start();

    LifecycleAware service = new MockLifecycleAware();
    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();

    supervisor.supervise(service, policy, LifecycleState.START);
    supervisor.unsupervise(service);

    service.stop();

    supervisor.stop();
  }

  @Test
  public void testStopServce() throws LifecycleException, InterruptedException {
    supervisor.start();

    MockLifecycleAware service = spy(new MockLifecycleAware());
    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(service, policy, LifecycleState.START);

    verify(service, timeout(3200).times(1)).start();
    verify(service, never()).stop();

    supervisor.setDesiredState(service, LifecycleState.STOP);

    verify(service, timeout(3200).times(1)).stop();
    verify(service, times(1)).start();

    supervisor.stop();
  }

  public static class MockLifecycleAware implements LifecycleAware {

    private LifecycleState lifecycleState;

    public MockLifecycleAware() {
      lifecycleState = LifecycleState.IDLE;
    }

    @Override
    public void start() {
      lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {
      lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
      return lifecycleState;
    }

  }

}
