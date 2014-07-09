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

import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestResettableLineReader {

  private ResettableLineReader r;

  @After
  public void closeReader() throws IOException {
    if (r != null) {
      r.close();
    }
  }

  /**
   * Ensure that we can simply read chars.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testBasicRead() throws IOException {
    String expected = "This is gonna be great!";
    r = newReader(expected);
    String actual = IOUtils.toString(r);
    assertEquals(expected, actual);
  }

  /**
   * Ensure that we can read char by char.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testSingleChar() throws IOException {
    r = newReader("a");
    assertEquals(r.read(), 'a');
    assertEquals(r.read(), -1);
  }

  /**
   * Ensure that we can read lines terminated by LF.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadLineLF() throws IOException {
    String expected = "This is gonna be great!\nThis is truly gonna be great!";
    r = newReader(expected);
    assertEquals("This is gonna be great!", r.readLine());
    assertEquals("This is truly gonna be great!", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read lines terminated by CR.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadLineCR() throws IOException {
    String expected = "This is gonna be great!\rThis is truly gonna be great!";
    r = newReader(expected);
    assertEquals("This is gonna be great!", r.readLine());
    assertEquals("This is truly gonna be great!", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read lines terminated by CR + LF.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadLineCRLF() throws IOException {
    String expected = "This is gonna be great!\r\nThis is truly gonna be great!";
    r = newReader(expected);
    assertEquals("This is gonna be great!", r.readLine());
    assertEquals("This is truly gonna be great!", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read empty lines terminated by LF.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadEmptyLineLF() throws IOException {
    String expected = "\n";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
    expected = "\n\n";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read empty lines terminated by CR.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadEmptyLineCR() throws IOException {
    String expected = "\r";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
    expected = "\r\r";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read empty lines terminated by CR+LF.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadEmptyLineCRLF() throws IOException {
    String expected = "\r\n";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
    expected = "\r\n\r\n";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read empty lines terminated by CR+LF.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadEmptyLineLFCR() throws IOException {
    String expected = "\n\r";
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
    expected = "\n\r\n\r";// 3 lines
    r = newReader(expected);
    assertEquals("", r.readLine());
    assertEquals("", r.readLine());
    assertEquals("", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that line truncation works as expected.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testTruncate() throws IOException {
    String expected = "foobar\nqix\n";
    r = newReader(expected);
    assertEquals("foo", r.readLine(3));
    assertEquals("bar", r.readLine(3));
    assertEquals("qix", r.readLine(3));
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that we can read lines terminated by mixed types of line terminators.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReadLineMixedTerminators() throws IOException {
    String expected =
        "1. On the planet of Mars\n" +
        "2. They have clothes just like ours,\r" +
        "3. And they have the same shoes and same laces,\r\n" +
        // LF + CR should be considered as two separate lines
        "4. And they have the same charms and same graces,\n\r" +
        "5. And they have the same heads and same faces...\r\n" +
        " \r\n"; // an empty space
    r = newReader(expected);
    assertEquals("1. On the planet of Mars", r.readLine());
    assertEquals("2. They have clothes just like ours,", r.readLine());
    assertEquals("3. And they have the same shoes and same laces,", r.readLine());
    assertEquals("4. And they have the same charms and same graces,", r.readLine());
    assertEquals("", r.readLine());
    assertEquals("5. And they have the same heads and same faces...", r.readLine());
    assertEquals(" ", r.readLine());
    assertNull(r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure a reset() brings us back to the default mark (beginning of input)
   *
   * @throws java.io.IOException
   */
  @Test
  public void testReset() throws IOException {
    String expected = "This is gonna be great!";
    r = newReader(expected);
    assertEquals(expected, IOUtils.toString(r));
    r.reset();
    assertEquals(expected, IOUtils.toString(r));
  }

  /**
   * Ensure that marking and resetting works.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testMarkReset() throws IOException {
    String expected =
        "1. On the planet of Mars\n" +
        "2. They have clothes just like ours,\n" +
        "3. And they have the same shoes and same laces,\n" +
        "4. And they have the same charms and same graces...\n";
    r = newReader(expected);
    assertEquals("1. On the planet of Mars", r.readLine());
    r.reset();
    assertEquals("1. On the planet of Mars", r.readLine());
    r.mark();
    assertEquals("2. They have clothes just like ours,", r.readLine());
    r.reset();
    assertEquals("2. They have clothes just like ours,", r.readLine());
    assertEquals("3. And they have the same shoes and same laces,", r.readLine());
    assertEquals("4. And they have the same charms and same graces...", r.readLine());
    assertNull(r.readLine());
    r.reset();
    assertEquals("2. They have clothes just like ours,", r.readLine());
    assertEquals("3. And they have the same shoes and same laces,", r.readLine());
    assertEquals("4. And they have the same charms and same graces...", r.readLine());
    assertNull(r.readLine());
  }

  /**
   * Ensure that resuming reading with a different instance of
   * ResettableLineReader works as expected.
   * This test also brings up a special situation where mark()
   * is called in the middle of a CR+LF; in this case,
   * the LF will be considered a separate line terminator.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testResume() throws IOException {
    String expected =
        "1. On the planet of Mars\r\n" +
        "2. They have clothes just like ours,\r\n" +
        "3. And they have the same shoes and same laces,\r\n" +
        "4. And they have the same charms and same graces...\r\n";
    PositionTracker tracker = new TransientPositionTracker("foo");
    r = newReliableReader(expected, tracker);
    assertEquals("1. On the planet of Mars", r.readLine());
    assertEquals("2. They have clothes just like ours,", r.readLine());
    r.mark(); // mark will point to the next '\n'
    assertEquals("3. And they have the same shoes and same laces,", r.readLine());
    assertEquals("4. And they have the same charms and same graces...", r.readLine());
    assertNull(r.readLine());
    r.close();
    // new reader, same tracker - this will cause the reader state to be lost
    // and the next '\n' will not be recognized anymore as part of a CR + LF pair.
    r = newReliableReader(expected, tracker);
    // will resume from marked '\n' - hence the empty line
    assertEquals("", r.readLine());
    assertEquals("3. And they have the same shoes and same laces,", r.readLine());
    // truncate line and mark it
    assertEquals("4. And they ", r.readLine(12));
    r.mark();
    // read rest of line
    assertEquals("have the same charms and same graces...", r.readLine());
    assertNull(r.readLine());
    r.close();
    // new reader, same tracker
    r = newReliableReader(expected, tracker);
    // will resume from mark
    assertEquals("have the same charms and same graces...", r.readLine());
    assertNull(r.readLine());
  }

  private ResettableLineReader newReader(String input) throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(input.getBytes(Charsets.UTF_8));
    return new ResettableLineReader(new ResettableInputStreamReader(in, Charsets.UTF_8));
  }

  private ResettableLineReader newReliableReader(String expected, PositionTracker tracker) throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(expected.getBytes(Charsets.UTF_8));
    return new ResettableLineReader(new ResettableInputStreamReader(new SeekableReliableInputStream(in, tracker), Charsets.UTF_8));
  }

}
