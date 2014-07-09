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

import com.google.common.base.Preconditions;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

/**
 * <p>An {@link InputStream} {@link Reader} with {@link Resettable} capabilities.</p>
 *
 * <p>To be able to reset its position to a previously marked offset, this reader
 * requires that the underlying {@link InputStream} implement {@link Seekable}, and
 * reads from it character by character (except when reading Unicode surrogate pairs - see below).</p>
 *
 * <p>Because it reads character by character, this class performs poorly when compared to similar implementations,
 * such as Sun's StreamDecoder. Mark and reset capabilities come at that price.</p>
 *
 * <p>This implementation also makes the following assumptions:</p>
 *
 * <ol>
 *   <li>The underlying {@link InputStream} is not modified by another thread while being read;
 *   this class does <em>not</em> synchronize on it while reading.</li>
 * </ol>
 *
 * <p><strong>A note on surrogate pairs:</strong></p>
 *
 * <p>The logic for decoding surrogate pairs is as follows:
 * If no character has been decoded by a "normal" pass, and the buffer still has remaining bytes,
 * then an attempt is made to read 2 characters in one pass.
 * If it succeeds, then the first char (high surrogate) is returned;
 * the second char (low surrogate) is recorded internally,
 * and is returned at the next call to {@link #read()}.
 * If it fails, then it is assumed that EOF has been reached.</p>
 *
 * <p>Impacts on position, mark and reset: when a surrogate pair is decoded, the position
 * is incremented by the amount of bytes taken to decode the <em>entire</em> pair (usually, 4).
 * This is the most reasonable choice since it would not be advisable
 * to reset a stream to a position pointing to the second char in a pair of surrogates:
 * such a dangling surrogate would not be properly decoded without its counterpart.</p>
 *
 * <p>Thus the behaviour of mark and reset is as follows:</p>
 *
 * <ol>
 *   <li>If {@link #mark()} is called after a high surrogate pair has been returned by {@link #read()},
 *   the marked position will be that of the character <em>following</em> the low surrogate,
 *   <em>not</em> that of the low surrogate itself.</li>
 *   <li>If {@link #reset()} is called after a high surrogate pair has been returned by {@link #read()},
 *   the low surrogate is always returned by the next call to {@link #read()},
 *   <em>before</em> the stream is actually reset to the last marked position.</li>
 * </ol>
 *
 * <p>This ensures that no dangling high surrogate could ever be read as long as
 * the same instance is used to read the whole pair. <strong>However, if {@link #reset()}
 * is called after a high surrogate pair has been returned by {@link #read()},
 * and a new instance of ResettableFileInputStream is used to resume reading,
 * then the low surrogate char will be lost,
 * resulting in a corrupted sequence of characters (dangling high surrogate).</strong>
 * This situation is hopefully extremely unlikely to happen in real life.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ResettableInputStreamReader extends Reader implements Resettable {

  /**
   * The default size of the buffer used to store bytes read
   * from the underlying file. (128 Kb).
   */
  public static final int DEFAULT_BUFFER_SIZE = 16384;

  /**
   * The minimum size of the buffer used to store bytes read
   * from the underlying stream.
   * This reader does not behave correctly when given too small buffers;
   * 32 is more than enough to prevent errors.
   */
  public static final int MIN_BUFFER_SIZE = 32;

  private static final Logger logger = LoggerFactory.getLogger(ResettableInputStreamReader.class);

  /**
   * A reference to the underlying {@link InputStream}.
   */
  private final InputStream in;

  /**
   * A reference to the underlying {@link InputStream}, declared as {@link Seekable} to allow for
   * easy, type-safe access to the {@link Seekable} interface methods where required.
   * @see #in
   */
  private final Seekable seekable;

  /**
   * The {@link ByteBuffer} instance used to buffer bytes read from the underlying {@link InputStream}.
   * The buffer is created internally with the supplied length, or with a default size of {@link #DEFAULT_BUFFER_SIZE}.
   * It is allocated on the heap.
   */
  private final ByteBuffer buf;

  /**
   * The {@link CharBuffer} instance used to collect decoded chars.
   * Since It has a capacity of 2 to allow surrogate
   * pairs to be decoded.
   */
  private final CharBuffer charBuf;

  /**
   * The {@link CharsetDecoder} used to decode bytes into chars.
   */
  private final CharsetDecoder decoder;

  /**
   * The underlying stream's current position, in bytes.
   * This reflects the actual position, i.e. that pointing
   * to the next character to read, and not the current position
   * of the underlying stream, which may differ, since the
   * underlying stream is buffered.
   */
  private long position;

  /**
   * The maximum number of bytes that a char may require in the specified character set.
   * That much bytes must always be available in the byte buffer to ensure that
   * multi-byte characters are properly decoded.
   */
  private int maxCharWidth;

  /**
   * Whether this instance holds a low surrogate character.
   */
  private boolean hasLowSurrogate = false;

  /**
   * A low surrogate character read from a surrogate pair.
   * When a surrogate pair is found, the high (first) surrogate pair
   * is immediately returned,
   * while the low (second) surrogate remains stored in memory,
   * to be returned at the next call to {@link #read()}.
   */
  private char lowSurrogate;

  /**
   * @param in
   *        {@link InputStream} instance to read bytes from. Must implement {@link Seekable},
   *        otherwise an {@link IllegalArgumentException} is thrown.
   *
   * @param charset
   *        Character set to use for decoding.
   *
   * @throws IOException
   *         If an I/O error occurs while querying the supplied {@link InputStream} for its current position.
   *
   * @throws IllegalArgumentException
   *         If the supplied {@link InputStream} does not implement {@link Seekable}
   */
  public ResettableInputStreamReader(InputStream in, Charset charset) throws IOException {
    this(in, charset.newDecoder(), DEFAULT_BUFFER_SIZE);
  }

  /**
   * @param in
   *        {@link InputStream} instance to read bytes from. Must implement {@link Seekable},
   *        otherwise an {@link IllegalArgumentException} is thrown.
   *
   * @param decoder
   *        {@link CharsetDecoder} instance to use to decode bytes into chars.
   *
   * @throws IOException
   *         If an I/O error occurs while querying the supplied {@link InputStream} for its current position.
   *
   * @throws IllegalArgumentException
   *         If the supplied {@link InputStream} does not implement {@link Seekable}
   */
  public ResettableInputStreamReader(InputStream in, CharsetDecoder decoder) throws IOException {
    this(in, decoder, DEFAULT_BUFFER_SIZE);
  }

  /**
   * @param in
   *        {@link InputStream} instance to read bytes from. Must implement {@link Seekable},
   *        otherwise an {@link IllegalArgumentException} is thrown.
   *
   * @param charset
   *        Character set to use for decoding.
   *
   * @param bufSize
   *        Size of the underlying buffer used for input. If lesser than {@link #MIN_BUFFER_SIZE},
   *        a buffer of length {@link #MIN_BUFFER_SIZE} will be created instead.
   *
   * @throws IOException
   *         If an I/O error occurs while querying the supplied {@link InputStream} for its current position.
   *
   * @throws IllegalArgumentException
   *         If the supplied {@link InputStream} does not implement {@link Seekable}
   */
  public ResettableInputStreamReader(InputStream in, Charset charset, int bufSize) throws IOException {
    this(in, charset.newDecoder(), bufSize);
  }

  /**
   * @param in
   *        {@link InputStream} instance to read bytes from. Must implement {@link Seekable},
   *        otherwise an {@link IllegalArgumentException} is thrown.
   *
   * @param decoder
   *        {@link CharsetDecoder} instance to use to decode bytes into chars.
   *
   * @param bufSize
   *        Size of the underlying buffer used for input. If lesser than {@link #MIN_BUFFER_SIZE},
   *        a buffer of length {@link #MIN_BUFFER_SIZE} will be created instead.
   *
   * @throws IOException
   *         If an I/O error occurs while querying the supplied {@link InputStream} for its current position.
   *
   * @throws IllegalArgumentException
   *         If the supplied {@link InputStream} does not implement {@link Seekable}
   */
  public ResettableInputStreamReader(InputStream in, CharsetDecoder decoder, int bufSize) throws IOException {
    if( ! (in instanceof Seekable)) {
      throw new IllegalArgumentException("This reader requires that the underlying InputStream implement Seekable");
    }
    this.in = in;
    this.seekable = (Seekable) in;
    this.decoder = decoder;
    // synchronize position with underlying stream
    this.position = this.seekable.tell();
    this.buf = ByteBuffer.allocate(bufSize < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : bufSize);
    this.buf.flip();
    this.charBuf = CharBuffer.allocate(2); // two chars for surrogate pairs
    this.charBuf.flip();
    this.hasLowSurrogate = false;
    Charset charset = decoder.charset();
    if(charset.name().startsWith("UTF-8")) {
      // some JDKs wrongly report 3 bytes max
      this.maxCharWidth = 4;
    } else if(charset.name().startsWith("UTF-16")) {
      // UTF_16BE and UTF_16LE wrongly report 2 bytes max
      this.maxCharWidth = 4;
    } else if(charset.name().startsWith("UTF-32")) {
      // UTF_32BE and UTF_32LE wrongly report 4 bytes max
      this.maxCharWidth = 8;
    } else {
      // rely on what is re
      this.maxCharWidth = (int) Math.ceil(charset.newEncoder().maxBytesPerChar());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() throws IOException {
    return readInternal(1);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note: this implementation is not very efficient as it
   * still reads character by character, delegating the actual
   * job to {@link #read()}.</p>
   */
  @Override
  public int read(char[] b, int off, int len) throws IOException {
    if(len > b.length - off) {
      len = b.length - off;
    }
    int n = 0;
    for(int i = off; i < off + len; i++) {
      int c = read();
      if(c == -1) {
        break;
      }
      b[i] = (char) c;
      n++;
    }
    return n == 0 ? -1 : n;
  }

  /**
   * Tells whether this stream supports the mark() operation.
   * This implementation returns <code>true</code>.
   * @return <code>true</code>.
   */
  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note: As this class implements {@link Resettable},
   * this implementation simply ignores the <code>readAheadLimit</code> parameter
   * and marks the current position.</p>
   *
   * @see #mark()
   */
  @Override
  public void mark(int readAheadLimit) throws IOException {
    seekable.mark(position);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark() throws IOException {
    seekable.mark(position);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    logger.trace("Reset to position: {}", seekable.point());
    // check to see if we can seek within our existing buffer
    long relativeChange = seekable.point() - this.position;
    if (relativeChange == 0) return; // seek to current pos => no-op
    long newBufPos = buf.position() + relativeChange;
    if (newBufPos >= 0 && newBufPos < buf.limit()) {
      // we can reuse the read buffer
      buf.position((int)newBufPos);
    } else {
      // otherwise, we have to invalidate the read buffer
      buf.clear();
      buf.flip();
    }
    // synchronize underlying stream
    seekable.reset();
    // clear decoder state
    decoder.reset();
    // reset position pointers
    this.position = seekable.point();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    in.close();
  }

  /**
   * Internal read method. Attempts to read <code>len</code> characters from the underlying stream.
   * @param len Maximum number of characters to read. Could be 1 (normal case) or 2 (surrogate pair).
   * @return The character read. In case of a surrogate pair, returns the high (first) surrogate.
   * @throws IOException
   */
  private int readInternal(int len) throws IOException {

    // Check whether we are in the middle of a surrogate pair,
    // in which case, return the last (low surrogate) char of the pair.
    if(hasLowSurrogate) {
      hasLowSurrogate = false;
      return lowSurrogate;
    }

    // The decoder can have issues with multi-byte characters.
    // This check ensures that there are at least maxCharWidth bytes in the buffer
    // before reaching EOF.
    if (buf.remaining() < maxCharWidth && seekable.remaining() > 0) {
      buf.clear();
      buf.flip();
      refillBuf();
    }

    charBuf.clear();
    charBuf.limit(len);

    int start = buf.position();
    boolean isEndOfInput = seekable.remaining() <= 0;
    CoderResult res = decoder.decode(buf, charBuf, isEndOfInput);

    if (res.isMalformed() || res.isUnmappable()) {
      res.throwException();
    }

    int delta = buf.position() - start;
    charBuf.flip();

    if (charBuf.remaining() == 1) {
      // Decoded a single char
      char c = charBuf.get();
      position += delta;
      return c;

    } else if (charBuf.remaining() == 2) {
      // Decoded two chars
      char highSurrogate = charBuf.get();
      // save second (low surrogate) char for later consumption
      lowSurrogate = charBuf.get();
      // Check if we really have a surrogate pair
      if( ! Character.isHighSurrogate(highSurrogate) || ! Character.isLowSurrogate(lowSurrogate)) {
        // This should only happen in case of bad sequences (dangling surrogate, etc.)
        logger.warn("Decoded a pair of chars, but it does not seem to be a surrogate pair: {} {}", (int)highSurrogate, (int)lowSurrogate);
      }
      hasLowSurrogate = true;
      // consider the pair as a single unit and increment position normally
      position += delta;
      // return the first (high surrogate) char of the pair
      return highSurrogate;

    } else if(len == 1 && res.isOverflow()) {
      // Decoded nothing, but the decoder indicates an overflow.
      // This situation denotes the presence of a surrogate pair
      // that can only be decoded if we have a 2-char buffer.
      // Increase the charBuf limit to 2 and try again.
      return readInternal(2);

    } else {
      // end of file
      position += delta;
      decoder.reset();
      return -1;

    }
  }

  private void refillBuf() throws IOException {
    buf.compact();
    seekable.seek(position);
    int n = in.read(buf.array(), buf.arrayOffset(), buf.limit());
    if(n < 0) return;
    buf.position(n);
    buf.flip();
  }

}