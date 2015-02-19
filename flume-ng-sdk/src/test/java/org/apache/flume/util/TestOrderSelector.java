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
package org.apache.flume.util;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.fest.assertions.Delta;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.agent.PowerMockAgent;
import org.powermock.modules.junit4.rule.PowerMockRule;

@PrepareForTest({OrderSelector.class, RoundRobinOrderSelector.class, RandomOrderSelector.class})
public class TestOrderSelector {

  static {
    PowerMockAgent.initializeIfNeeded();
  }

  @Rule
  public PowerMockRule powerMock = new PowerMockRule();

  @Before
  public void setUp() throws Exception {
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

  private <T> boolean checkSelector(final OrderSelector<T> selector, T...elements) {
    return checkSelector(true, selector, elements);
  }

  private <T> boolean checkSelector(final boolean useAssert, final OrderSelector<T> selector, T...elements) {
    Iterator<T> it = selector.createIterator();
    boolean allAreEqual = true;
    List<T> elementList = new ArrayList<T>(Arrays.asList(elements));
    for (final T element : elements) {
      final T actual = it.next();
      if (useAssert) {
        assertThat(actual).isEqualTo(element);
      } else {
        assertThat(elementList).contains(element);
        elementList.remove(element);
      }
      allAreEqual &= actual.equals(element);
    }
    assertThat(it.hasNext()).isFalse();
    return allAreEqual;
  }

  @Test
  public void testRoundRobinOrderSelector() throws Exception {
    final RoundRobinOrderSelector<Integer> selector = new RoundRobinOrderSelector<Integer>(false);
    selector.setObjects(Arrays.asList(1, 2, 3, 4, 5));
    checkSelector(selector, 1, 2, 3, 4, 5);
    checkSelector(selector, 2, 3, 4, 5, 1);
    checkSelector(selector, 3, 4, 5, 1, 2);
    selector.informFailure(4);
    checkSelector(selector, 4, 5, 1, 2, 3);
    selector.informFailure(4);
    selector.informFailure(5);
    selector.informFailure(1);
    checkSelector(selector, 5, 1, 2, 3, 4);
    checkSelector(selector, 1, 2, 3, 4, 5);
  }

  @Test
  public void testRoundRobinOrderSelectorWithBackoff() throws Exception {
    setCurrentTimeMillis(0);
    final RoundRobinOrderSelector<Integer> selector = new RoundRobinOrderSelector<Integer>(true);
    selector.setObjects(Arrays.asList(1, 2, 3, 4, 5));
    checkSelector(selector, 1, 2, 3, 4, 5);
    checkSelector(selector, 2, 3, 4, 5, 1);
    checkSelector(selector, 3, 4, 5, 1, 2);
    selector.informFailure(4);
    checkSelector(selector, 5, 1, 2, 3);
    checkSelector(selector, 1, 2, 3, 5);
    checkSelector(selector, 2, 3, 5, 1);
    selector.informFailure(3);
    selector.informFailure(5);
    checkSelector(selector, 1, 2);
    selector.informFailure(1);
    selector.informFailure(2);
    checkSelector(selector);
    setCurrentTimeMillis(2000);
    checkSelector(selector);
    setCurrentTimeMillis(2001);
    checkSelector(selector, 2, 3, 4, 5, 1);
  }

  @Test
  public void testRandomOrderSelector() throws Exception {
    final RandomOrderSelector<Integer> selector = new RandomOrderSelector<Integer>(false);
    selector.setObjects(Arrays.asList(1, 2, 3));
    int timesEqual = 0;
    double total = 500.0;
    for (int i = 0; i < total; i++) {
      if (checkSelector(false, selector, 1, 2, 3)) {
        timesEqual++;
      }
    }
    assertThat(timesEqual / total).isEqualTo(0.165, Delta.delta(0.1));
    selector.informFailure(2);
    selector.informFailure(3);
    selector.informFailure(1);
    checkSelector(false, selector, 1, 2, 3);
  }

  @Test
  public void testRandomOrderSelectorWithBackoff() throws Exception {
    setCurrentTimeMillis(0);
    final RandomOrderSelector<Integer> selector = new RandomOrderSelector<Integer>(true);
    selector.setObjects(Arrays.asList(1, 2, 3));
    int timesEqual = 0;
    double total = 200.0;
    for (int i = 0; i < total; i++) {
      if (checkSelector(false, selector, 1, 2, 3)) {
        timesEqual++;
      }
    }
    assertThat(timesEqual / total).isEqualTo(0.165, Delta.delta(0.1));
    selector.informFailure(2);
    timesEqual = 0;
    for (int i = 0; i < total; i++) {
      if (checkSelector(false, selector, 1, 3)) {
        timesEqual++;
      }
    }
    assertThat(timesEqual / total).isEqualTo(0.5, Delta.delta(0.1));
    setCurrentTimeMillis(2001);
    checkSelector(false, selector, 1, 2, 3);
    selector.informFailure(3);
    checkSelector(false, selector, 1, 2);
    selector.informFailure(2); // Two sequential fails will lead to a backoff of 4000ms
    setCurrentTimeMillis(4001);
    checkSelector(false, selector, 1);
    setCurrentTimeMillis(6001);
    checkSelector(false, selector, 1, 3);
    setCurrentTimeMillis(6002);
    checkSelector(false, selector, 1, 2, 3);
  }

}
