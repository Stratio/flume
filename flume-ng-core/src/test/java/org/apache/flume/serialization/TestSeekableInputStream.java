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

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

public abstract class TestSeekableInputStream<T extends InputStream & Seekable> {

  protected static final byte[] CONTENTS = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

  protected T in;

  @Before
  public void setUpInputStream() throws IOException {
    this.in = newInputStream(CONTENTS);
  }

  @After
  public void closeInputStream() throws IOException {
    if(in != null) {
      in.close();
    }
  }

  @Test
  public void testAvailable() throws IOException {
    for(int expected = CONTENTS.length; expected >= 0; expected--, in.read()) {
      assertEquals(expected, in.available());
    }
    assertEquals(0, in.available());
    assertEquals(-1, in.read());
  }

  @Test
  public void testMarkSupported() {
    assertTrue(in.markSupported());
  }

  @Test
  public void testMarkReadAheadLimit() throws IOException {
    in.read();
    in.mark(-1);
    int expected = in.read();
    in.read();
    in.reset();
    assertEquals(expected, in.read());
  }

  @Test
  public void testMark() throws IOException {
    in.read();
    in.read();
    in.read();
    in.mark();
    in.read();
    in.read();
    in.reset();
    assertEquals(codePointAt(3), in.read());
  }

  @Test
  public void testMarkPosition() throws IOException {
    in.read();
    in.read();
    in.read();
    in.mark(1L);
    in.read();
    in.read();
    in.read();
    in.reset();
    assertEquals(codePointAt(1), in.read());
  }

  @Test
  public void testPoint() throws IOException {
    assertEquals(0L, in.point());
    in.mark(3L);
    assertEquals(3L, in.point());
    in.mark(-1L);
    assertEquals(-1L, in.point());
    in.mark(1000L);
    assertEquals(1000L, in.point());
  }

  @Test
  public void testReset() throws IOException {
    in.reset();
    assertEquals(codePointAt(0), in.read());
    in.mark(0L);
    in.reset();
    assertEquals(codePointAt(0), in.read());
    in.mark(in.length());
    in.reset();
    assertEquals(-1, in.read());
  }

  @Test(expected = Exception.class)
  public void testResetNegativeMark() throws IOException {
    in.mark(-1L);
    in.reset();
    in.read();
    fail("Should not allow reset to negative mark");
  }

  @Test(expected = Exception.class)
  public void testResetOutOfBounds() throws IOException {
    in.mark(1000L);
    in.reset();
    in.read();
    fail("Should not allow reset to out-of-bounds mark");
  }

  @Test
  public void testSeekPosition() throws IOException {
    for(long position = 0; position < in.length(); position++) {
      in.seek(position);
      assertEquals(codePointAt((int)position), in.read());
    }
    in.seek(in.length());
    assertEquals(-1, in.read());
  }

  @Test(expected = Exception.class)
  public void testSeekNegativePosition() throws IOException {
    in.seek(-1L);
    in.read();
    fail("Should not allow seek of negative position");
  }

  @Test(expected = Exception.class)
  public void testSeekOutOfBoundsPosition() throws IOException {
    in.seek(1000L);
    in.read();
    fail("Should not allow seek to out-of-bounds position");
  }

  @Test
  public void testTell() throws IOException {
    assertEquals(0, in.tell());
    in.seek(2);
    assertEquals(2, in.tell());
    in.seek(in.length());
    assertEquals(in.length(), in.tell());
  }

  @Test
  public void testLength() throws IOException {
    assertEquals(CONTENTS.length, in.length());
  }

  @Test
  public void testRemaining() throws IOException {
    for(int expected = CONTENTS.length; expected >= 0; expected--, in.read()) {
      assertEquals(expected, in.remaining());
    }
    assertEquals(0, in.remaining());
    assertEquals(-1, in.read());
  }

  protected abstract T newInputStream(byte[] contents) throws IOException;

  protected int codePointAt(int index) {
    return CONTENTS[index] & 0xFF;
  }

  protected byte[] consumeFully(InputStream in) throws IOException {
    return IOUtils.toByteArray(in);
  }

}
