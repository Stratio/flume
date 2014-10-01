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

import static com.google.common.base.Charsets.*;
import static java.nio.charset.CodingErrorAction.*;

import com.google.common.base.Charsets;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;

import static org.junit.Assert.*;

public class TestResettableInputStreamReader {

  /**
   * FACE THROWING A KISS (Unicode character outside Unicode's BMP).
   * Needs a surrogate pair.
   * U+1F618
   * UTF-8 : f0 9f 98 98
   * UTF-16: d8 3d de 18
   */
  private static final String FACE_THROWING_A_KISS = "\ud83d\ude18";

  /**
   * GUJARATI LETTER O. Needs 3 bytes in UTF-8.
   * U+0A93
   * UTF-8: e0 aa 93
   */
  private static final char GUJARATI_LETTER_O = '\u0a93';

  /**
   * Letters from ISO-8859-1 (c0 to ff).
   */
  private static final String ISO_8859_1_LETTERS =
      "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ";

  /**
   * A valid UTF-8 5-byte sequence, but not Unicode.
   */
  private static final byte[] UTF_8_NON_UNICODE_SEQUENCE =
      new byte[]{(byte) 0xf8, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1};

  /**
   * UTF-16 Byte Order Mark (BOM) - Big Endian.
   */
  private static final byte[] UTF_16_BE_BOM = new byte[]{(byte) 0xFE, (byte) 0xFF};

  /**
   * First byte of a 2-byte sequence.
   */
  private static final byte[] UTF_8_MALFORMED_SEQUENCE = new byte[]{(byte) 0xc0};

  /**
   * UTF-8 dangling high surrogate.
   */
  private static final byte[] UTF_8_DANGLING_SURROGATE = new byte[]{(byte) 0xf0, (byte) 0x9f};

  /**
   * UTF-8 encoded, overly-long slash character - should not be accepted.
   */
  private static final byte[] UTF_8_OVERLY_LONG_SEQUENCE = new byte[]{(byte) 0xe0, (byte) 0x80, (byte) 0xaf};

  /**
   * A control char in in ISO-8859-1 : HOP - High Octet Preset (0x81).
   * Such control characters are rarely if ever used, and their presence
   * is likely to indicate a badly encoded sequence.
   */
  private static final byte[] ISO_8859_1_HOP = new byte[]{(byte) 0x81};

  /**
   * Invalid encoding in US-ASCII (0x81 is not mapped).
   */
  private static final byte[] ASCII_INVALID_SEQUENCE = new byte[]{(byte) 0x81};

  private static final char UTF_8_REPLACEMENT_CHAR = '\uFFFD';

  private ResettableInputStreamReader r;

  private ResettableInputStreamReaderBuilder builder;

  @Before
  public void setUpBuilder() {
    builder = new ResettableInputStreamReaderBuilder();
  }

  @After
  public void closeReader() throws IOException {
    if (r != null) {
      r.close();
    }
  }

  /**
   * Ensure that we can read the entire stream.
   * @throws java.io.IOException
   */
  @Test
  public void testBasicRead() throws IOException {
    r = builder.append("This is gonna be great!").build();
    String actual = IOUtils.toString(r);
    assertEquals(builder.expected(), actual);
  }

  /**
   * Ensure that we cannot build a reader if
   * the underlying input stream does not implement Seekable.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNonSeekableInputStream() throws IOException {
    new ResettableInputStreamReader(new ByteArrayInputStream(new byte[] {}), Charsets.UTF_8);
  }

  /**
   * Ensure that we can read char by char.
   * @throws java.io.IOException
   */
  @Test
  public void testSingleChar() throws IOException {
    r = builder.append("a").build();
    assertEquals('a', r.read());
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that we can read char by char even with multi-byte chars.
   * @throws java.io.IOException
   */
  @Test
  public void testMultiByteSingleChar() throws IOException {
    r = builder.append("ö").build();
    assertEquals('ö', r.read());
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that read(char[], int, int) works as expected.
   * @throws java.io.IOException
   */
  @Test
  public void testCharArrayRead() throws IOException {
    r = builder.append("abc").build();
    char[] buf = new char[3];
    // write 3 chars from offset 2 -> will actually write 1 char only
    int read = r.read(buf, 2, 3);
    assertEquals(1, read);
    assertEquals('a', buf[2]);
    // write 3 chars from offset 0 -> will actually write 2 chars only
    read = r.read(buf, 0, 3);
    assertEquals(2, read);
    assertEquals('b', buf[0]);
    assertEquals('c', buf[1]);
    // write 3 chars from offset 0 -> will actually not write any chars
    read = r.read(buf, 0, 3);
    assertEquals(-1, read);
  }

  /**
   * Ensure that we can read a surrogate pair in UTF-8 char by char.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8SurrogatePair() throws IOException {
    r = builder
        .charset(UTF_8)
        .append(FACE_THROWING_A_KISS)
        .build();
    assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read()); // high surrogate
    assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read()); // low surrogate
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that we can read a surrogate pair in UTF-16 Big-Endian char by char.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf16BESurrogatePair() throws IOException {
    r = builder
        .charset(UTF_16BE)
        .append(FACE_THROWING_A_KISS)
        .build();
    assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read()); // high surrogate
    assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read()); // low surrogate
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that we can read a surrogate pair in UTF-16 Low-Endian char by char.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf16LESurrogatePair() throws IOException {
    r = builder
        .charset(UTF_16LE)
        .append(FACE_THROWING_A_KISS)
        .build();
    assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read()); // high surrogate
    assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read()); // low surrogate
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that we can read a surrogate pair in UTF-32 Big-Endian char by char.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf32BESurrogatePair() throws IOException {
    r = builder
        .charset("UTF_32BE")
        .append(FACE_THROWING_A_KISS)
        .build();
    assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read()); // high surrogate
    assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read()); // low surrogate
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that we can read a surrogate pair in UTF-32 Low-Endian char by char.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf32LESurrogatePair() throws IOException {
    r = builder
        .charset("UTF_32LE")
        .append(FACE_THROWING_A_KISS)
        .build();
    assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read()); // high surrogate
    assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read()); // low surrogate
    assertEquals(-1, r.read());
  }

  /**
   * Ensure that the reader understands Byte Order Marks in UTF-16.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf16BOM() throws IOException {
    r = builder
        .charset(UTF_16BE) // no BOM
        .append(UTF_16_BE_BOM) // manually add a BOM
        .append("foo")
        .charset(UTF_16) // so that the reader will decode BOM
        .build();
    assertEquals("foo", IOUtils.toString(r));
  }

  /**
   * Ensure that we can read chars with ISO-8859-1 encoding.
   * @throws java.io.IOException
   */
  @Test
  public void testBasicLatin1() throws IOException {
    r = builder
        .charset(ISO_8859_1)
        .append(ISO_8859_1_LETTERS)
        .build();
    String actual = IOUtils.toString(r);
    assertEquals(builder.expected(), actual);
  }

  /**
   * Ensure that we can process lines that contain multi byte characters in weird places
   * such as at the end of a buffer.
   * @throws java.io.IOException
   */
  @Test
  public void testMultiByteCharAstrideBufferBoundaries() throws IOException {
    r = builder
        .charset(UTF_8)
        .bufferSize(ResettableInputStreamReader.MIN_BUFFER_SIZE)
        .append(" ", ResettableInputStreamReader.MIN_BUFFER_SIZE - 2)
        .append(GUJARATI_LETTER_O)
        .build();
    String actual = IOUtils.toString(r);
    assertEquals(builder.expected(), actual);
  }

  /**
   * Ensure that we can process UTF-8 input that contains a surrogate pair
   * appearing astride buffer boundaries.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8SurrogatePairAstrideBufferBoundaries() throws IOException {
    r = builder
        .charset(UTF_8)
        .bufferSize(ResettableInputStreamReader.MIN_BUFFER_SIZE)
        .append(" ", ResettableInputStreamReader.MIN_BUFFER_SIZE - 3)
        .append(FACE_THROWING_A_KISS)
        .build();
    String actual = IOUtils.toString(r);
    assertEquals(builder.expected(), actual);
  }

  /**
   * Ensure that we can process UTF-16 input that contains a
   * surrogate pair appearing astride buffer boundaries.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf16SurrogatePairAstrideBufferBoundaries() throws IOException {
    r = builder
        .charset(UTF_16BE) // no BOM
        .bufferSize(ResettableInputStreamReader.MIN_BUFFER_SIZE)
        .append(" ", 15) // will take up 30 bytes in UTF-16
        .append(FACE_THROWING_A_KISS)
        .build();
    String actual = IOUtils.toString(r);
    assertEquals(builder.expected(), actual);
  }

  /**
   * Ensure that reader will throw MalformedInputException on a malformed UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test(expected = MalformedInputException.class)
  public void testUtf8DecodeErrorHandlingFailMalformed() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPORT)
        .append(UTF_8_MALFORMED_SEQUENCE)
        .build();
    IOUtils.toString(r);
    fail("Expected MalformedInputException!");
  }

  /**
   * Ensure that reader will throw MalformedInputException on a dangling surrogate.
   * @throws java.io.IOException
   */
  @Test(expected = MalformedInputException.class)
  public void testUtf8DecodeErrorHandlingFailDanglingSurrogate() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPORT)
        .append(UTF_8_DANGLING_SURROGATE)
        .build();
    IOUtils.toString(r);
    fail("Expected MalformedInputException!");
  }

  /**
   * Ensure that reader will throw MalformedInputException on an overly long UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test(expected = MalformedInputException.class)
  public void testUtf8DecodeErrorHandlingFailOverlyLongSequence() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPORT)
        .append(UTF_8_OVERLY_LONG_SEQUENCE)
        .build();
    IOUtils.toString(r);
    fail("Expected MalformedInputException!");
  }

  /**
   * Ensure that reader will throw MalformedInputException on a wrongly-encoded UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test(expected = MalformedInputException.class)
  public void testUtf8DecodeErrorHandlingFailBadEncoding() throws IOException {
    r = builder
        .charset(ISO_8859_1)
        .action(REPORT)
        .append("é")
        .charset(UTF_8)
        .build();
    IOUtils.toString(r);
    fail("Expected MalformedInputException!");
  }

  /**
   * Ensure that reader will throw MalformedInputException on a non-Unicode UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test(expected = MalformedInputException.class)
  public void testUtf8DecodeErrorHandlingFailNonUnicode() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPORT)
        .append(UTF_8_NON_UNICODE_SEQUENCE)
        .build();
    IOUtils.toString(r);
    fail("Expected MalformedInputException!");
  }

  /**
   * Ensure that reader will ignore a malformed UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingIgnoreMalformed() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(IGNORE)
        .append("foo")
        .append(UTF_8_MALFORMED_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foobar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will ignore a dangling surrogate.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingIgnoreDanglingSurrogate() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(IGNORE)
        .append("foo")
        .append(UTF_8_DANGLING_SURROGATE)
        .append("bar")
        .build();
    assertEquals("foobar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will ignore an overly long UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingIgnoreOverlyLongSequence() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(IGNORE)
        .append("foo")
        .append(UTF_8_OVERLY_LONG_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foobar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will ignore a wrongly-encoded UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingIgnoreBadEncoding() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(IGNORE)
        .append("foo")
        .charset(ISO_8859_1)
        .append("é")
        .charset(UTF_8)
        .append("bar")
        .build();
    assertEquals("foobar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will ignore a non-Unicode UTF-8 sequence.
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingIgnoreNonUnicode() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(IGNORE)
        .append("foo")
        .append(UTF_8_NON_UNICODE_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foobar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will replace a malformed UTF-8 sequence with the replacement char ('?').
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingReplaceMalformed() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPLACE)
        .append("foo")
        .append(UTF_8_MALFORMED_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foo" + UTF_8_REPLACEMENT_CHAR + "bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will replace a dangling surrogate with the replacement char ('?').
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingReplaceDanglingSurrogate() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPLACE)
        .append("foo")
        .append(UTF_8_DANGLING_SURROGATE)
        .append("bar")
        .build();
    assertEquals("foo" + UTF_8_REPLACEMENT_CHAR + "bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will replace an overly long UTF-8 sequence with the replacement char ('?').
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingReplaceOverlyLongSequence() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPLACE)
        .append("foo")
        .append(UTF_8_OVERLY_LONG_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foo" + UTF_8_REPLACEMENT_CHAR + UTF_8_REPLACEMENT_CHAR + UTF_8_REPLACEMENT_CHAR + "bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will replace a wrongly-encoded UTF-8 sequence with the replacement char ('?').
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingReplaceBadEncoding() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPLACE)
        .append("foo")
        .charset(ISO_8859_1) // to force a wrong encoding
        .append("é")
        .charset(UTF_8)
        .append("bar")
        .build();
    assertEquals("foo" + UTF_8_REPLACEMENT_CHAR + "bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will replace a non-Unicode UTF-8 sequence with the replacement char ('?').
   * @throws java.io.IOException
   */
  @Test
  public void testUtf8DecodeErrorHandlingReplaceNonUnicode() throws IOException {
    r = builder
        .charset(UTF_8)
        .action(REPLACE)
        .append("foo")
        .append(UTF_8_NON_UNICODE_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foo" + UTF_8_REPLACEMENT_CHAR + "bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will never throw MalformedInputException on a Latin-1 sequence,
   * even a probably ill-formatted one,
   * because Latin-1 is a one-byte encoding where all positions are mapped.
   * @throws java.io.IOException
   */
  @Test
  public void testLatin1DecodeErrorHandlingFailMalformed() throws IOException {
    r = builder
        .charset(ISO_8859_1)
        .action(REPORT)
        .append("foo")
        .append(ISO_8859_1_HOP)
        .append("bar")
        .build();
    // ISO 8859-1 decoder NEVER reports errors
    assertEquals("foo\u0081bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will never ignore a Latin-1 sequence,
   * even a probably ill-formatted one,
   * because Latin-1 is a one-byte encoding where all positions are mapped.
   * @throws java.io.IOException
   */
  @Test
  public void testLatin1DecodeErrorHandlingIgnore() throws IOException {
    r = builder
        .charset(ISO_8859_1)
        .action(IGNORE)
        .append("foo")
        .append(ISO_8859_1_HOP)
        .append("bar")
        .build();
    // ISO 8859-1 decoder NEVER reports errors
    assertEquals("foo\u0081bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will never replace a Latin-1 sequence,
   * even a probably ill-formatted one,
   * because Latin-1 is a one-byte encoding where all positions are mapped.
   * @throws java.io.IOException
   */
  @Test
  public void testLatin1DecodeErrorHandlingReplace() throws IOException {
    r = builder
        .charset(ISO_8859_1)
        .action(REPLACE)
        .append("foo")
        .append(ISO_8859_1_HOP)
        .append("bar")
        .build();
    // ISO 8859-1 decoder NEVER reports errors
    assertEquals("foo\u0081bar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will throw MalformedInputException on a bad ASCII sequence.
   * @throws java.io.IOException
   */
  @Test(expected = MalformedInputException.class)
  public void testAsciiDecodeErrorHandlingFailMalformed() throws IOException {
    r = builder
        .charset(US_ASCII)
        .action(REPORT)
        .append("foo")
        .append(ASCII_INVALID_SEQUENCE)
        .append("bar")
        .build();
    IOUtils.toString(r);
    fail("Expected MalformedInputException!");
  }

  /**
   * Ensure that reader will ignore a bad ASCII sequence.
   * @throws java.io.IOException
   */
  @Test
  public void testAsciiDecodeErrorHandlingIgnore() throws IOException {
    r = builder
        .charset(US_ASCII)
        .action(IGNORE)
        .append("foo")
        .append(ASCII_INVALID_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foobar", IOUtils.toString(r));
  }

  /**
   * Ensure that reader will replace a bad ASCII sequence by the default replacement char ('?').
   * @throws java.io.IOException
   */
  @Test
  public void testAsciiDecodeErrorHandlingReplace() throws IOException {
    r = builder
        .charset(US_ASCII)
        .action(REPLACE)
        .append("foo")
        .append(ASCII_INVALID_SEQUENCE)
        .append("bar")
        .build();
    assertEquals("foo" + UTF_8_REPLACEMENT_CHAR + "bar", IOUtils.toString(r));
  }

  /**
   * Ensure a reset() brings us back to the default mark (beginning of input)
   * @throws java.io.IOException
   */
  @Test
  public void testReset() throws IOException {
    r = builder
        .append("This is gonna be great!")
        .build();
    assertEquals(builder.expected(), IOUtils.toString(r));
    r.reset();
    assertEquals(builder.expected(), IOUtils.toString(r));
  }

  /**
   * Ensure a reset() with buffer invalidation works as expected
   * @throws java.io.IOException
   */
  @Test
  public void testResetWithBufferInvalidation() throws IOException {
    r = builder
        .charset(UTF_8)
        .bufferSize(ResettableInputStreamReader.MIN_BUFFER_SIZE)
        .append("The quick brown fox jumps over the lazy dog") // 43 bytes
        .build();
    // consume to whole input - will required a buffer refill
    IOUtils.toString(r);
    r.reset(); // will invalidate current buffer
    // will consume the whole input again
    assertEquals(builder.expected(), IOUtils.toString(r));
  }

  /**
   * Ensure that this reader works well with mark/reset,
   * even in presence of surrogate pairs.
   * @throws IOException
   */
  @Test
  public void testMarkReset() throws IOException {
    r = builder
        .charset(UTF_8)
        .append("foo")
        .append(GUJARATI_LETTER_O)
        .append(FACE_THROWING_A_KISS)
        .append("bar")
        .build();
    Assert.assertTrue(r.markSupported());
    Assert.assertEquals('f', r.read());
    Assert.assertEquals('o', r.read());
    r.mark(-1); // test that readAheadLimit is ignored
    Assert.assertEquals('o', r.read());
    // Gujarati letter O
    Assert.assertEquals(GUJARATI_LETTER_O, r.read());
    // read high surrogate
    Assert.assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read());
    // call reset in the middle of a surrogate pair
    r.reset();
    // will read low surrogate *before* reverting back to mark, to ensure
    // surrogate pair is properly read
    Assert.assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read());
    // now back to marked position
    Assert.assertEquals('o', r.read());
    // Gujarati letter O
    Assert.assertEquals(GUJARATI_LETTER_O, r.read());
    // read high surrogate again
    Assert.assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read());
    // call mark in the middle of a surrogate pair:
    // will mark the position *after* the pair, *not* low surrogate's position
    r.mark();
    // will reset to the position *after* the pair
    r.reset();
    // read low surrogate normally despite of reset being called
    // so that the pair is entirely read
    Assert.assertEquals(FACE_THROWING_A_KISS.charAt(1), r.read());
    Assert.assertEquals('b', r.read());
    Assert.assertEquals('a', r.read());
    // will reset to the position *after* the pair
    r.reset();
    Assert.assertEquals('b', r.read());
    Assert.assertEquals('a', r.read());
    Assert.assertEquals('r', r.read());
    Assert.assertEquals(-1, r.read());
  }

  /**
   * Ensure that resuming
   * reading with a different instance of ResettableInputStreamReader
   * works as expected.
   * More specifically, this test brings up special situations
   * where a surrogate pair cannot be correctly read because
   * the second character is lost.
   *
   * @throws IOException
   */
  @Test
  public void testResume() throws IOException {
    r = builder
        .charset(UTF_8)
        .reliable()
        .append("foo")
        .append(FACE_THROWING_A_KISS)
        .append("bar")
        .build();
    Assert.assertEquals('f', r.read());
    Assert.assertEquals('o', r.read());
    r.mark();
    Assert.assertEquals('o', r.read());
    // read high surrogate
    Assert.assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read());
    // call reset in the middle of a surrogate pair
    r.reset();
    // close reader
    r.close();
    // new reader, same tracker - this will cause the low surrogate char
    // stored in-memory to be lost
    r = builder.build();
    // low surrogate char is now lost - resume from marked position
    Assert.assertEquals('o', r.read());
    // read high surrogate again
    Assert.assertEquals(FACE_THROWING_A_KISS.charAt(0), r.read());
    // call mark in the middle of a surrogate pair:
    // will mark the position *after* the pair, *not* low surrogate's position
    r.mark();
    // close reader
    r.close();
    // new reader, same tracker - this will cause the low surrogate char
    // stored in-memory to be lost
    r = builder.build();
    // low surrogate char is now lost - resume from marked position
    Assert.assertEquals('b', r.read());
    Assert.assertEquals('a', r.read());
    Assert.assertEquals('r', r.read());
    Assert.assertEquals(-1, r.read());
  }

  /**
   * A small utility class to help build ResettableInputStreamReader instances.
   */
  private class ResettableInputStreamReaderBuilder {

    private ByteArrayOutputStream out = new ByteArrayOutputStream();

    private Charset charset = UTF_8;

    private CodingErrorAction action;

    private Integer bufferSize;

    private PositionTracker tracker;

    public ResettableInputStreamReaderBuilder clear() {
      out = new ByteArrayOutputStream();
      return this;
    }

    public ResettableInputStreamReaderBuilder reliable() {
      tracker = new TransientPositionTracker("foo");
      return this;
    }

    public ResettableInputStreamReaderBuilder charset(String charset) {
      this.charset = Charset.forName(charset);
      return this;
    }

    public ResettableInputStreamReaderBuilder charset(Charset charset) {
      this.charset = charset;
      return this;
    }

    public ResettableInputStreamReaderBuilder action(CodingErrorAction action) {
      this.action = action;
      return this;
    }

    public ResettableInputStreamReaderBuilder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public ResettableInputStreamReaderBuilder append(char c) throws IOException {
      return append(Character.toString(c));
    }

    public ResettableInputStreamReaderBuilder append(String str) throws IOException {
      this.out.write(str.getBytes(charset));
      return this;
    }

    public ResettableInputStreamReaderBuilder append(byte... bytes) throws IOException {
      this.out.write(bytes);
      return this;
    }

    public ResettableInputStreamReaderBuilder append(String str, int repeat) throws IOException {
      for (int i = 0; i < repeat; i++) {
        this.out.write(str.getBytes(charset));
      }
      return this;
    }

    public String expected() {
      return new String(this.out.toByteArray(), charset);
    }

    public ResettableInputStreamReader build() throws IOException {
      SeekableByteArrayInputStream seekable = new SeekableByteArrayInputStream(out.toByteArray());
      if(tracker == null) {
        return build(seekable);
      } else {
        return build(new SeekableReliableInputStream(seekable, tracker));
      }
    }

    private <T extends InputStream & Seekable> ResettableInputStreamReader build(T seekable) throws IOException {
      if(action != null && bufferSize != null) {
        return new ResettableInputStreamReader(seekable, decoder(), bufferSize);
      } else if(action != null) {
        return new ResettableInputStreamReader(seekable, decoder());
      } else if(bufferSize != null) {
        return new ResettableInputStreamReader(seekable, charset, bufferSize);
      } else {
        return new ResettableInputStreamReader(seekable, charset);
      }
    }

    private CharsetDecoder decoder() {
      CharsetDecoder decoder = charset.newDecoder();
      decoder.onMalformedInput(action);
      decoder.onUnmappableCharacter(action);
      return decoder;
    }

  }

}
