/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.serialization;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestSeekableReliableInputStream extends TestSeekableInputStream<SeekableReliableInputStream> {

  private PositionTracker tracker;

  @Override
  @Test
  public void testMark() throws IOException {
    assertEquals(0, tracker.getPosition());
    in.mark();
    assertEquals(0, tracker.getPosition());
    super.testMark();
    in.seek(11);
    in.mark();
    assertEquals(11, tracker.getPosition());
    verify(tracker).storePosition(0);
    verify(tracker).storePosition(11);
  }

  @Override
  @Test
  public void testMarkPosition() throws IOException {
    assertEquals(0, tracker.getPosition());
    in.mark(0L);
    assertEquals(0, tracker.getPosition());
    super.testMarkPosition();
    in.mark(11L);
    assertEquals(11, tracker.getPosition());
    verify(tracker).storePosition(0);
    verify(tracker).storePosition(11);
  }

  @Override
  @Test
  public void testMarkReadAheadLimit() throws IOException {
    assertEquals(0, tracker.getPosition());
    in.mark(-1);
    assertEquals(0, tracker.getPosition());
    super.testMarkPosition();
    in.seek(11);
    in.mark(-1); // ignore -1
    assertEquals(11, tracker.getPosition());
    verify(tracker).storePosition(0);
    verify(tracker).storePosition(11);
    // special corner case where an IOE would be wrapped in a RuntimeException
    doThrow(IOException.class).when(tracker).storePosition(10);
    in.seek(10);
    try {
      in.mark(1); // will trigger IOException on the tracker mock
      fail("Should have thrown a Runtime exception");
    } catch(RuntimeException e) {}
  }

  @Override
  @Test
  public void testPoint() throws IOException {
    assertEquals(in.point(), tracker.getPosition());
    super.testPoint();
    assertEquals(in.point(), tracker.getPosition());
  }

  @Override
  @Test
  public void testReset() throws IOException {
    super.testReset();
    verify(tracker).storePosition(0);
    verify(tracker).storePosition(in.length());
  }

  /**
   * Ensure that we cannot build an input stream if
   * the underlying input stream does not implement Seekable.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNonSeekableInputStream() throws IOException {
    new SeekableReliableInputStream(new ByteArrayInputStream(new byte[] {}), tracker);
  }

  @Override
  protected SeekableReliableInputStream newInputStream(byte[] contents) throws IOException {
    tracker = mock(TransientPositionTracker.class, CALLS_REAL_METHODS);
    return new SeekableReliableInputStream(new SeekableByteArrayInputStream(contents), tracker);
  }

}
