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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>A subclass of {@link SeekableInputStream} that provides buffering capabilities
 * while keeping the ability to report its current position at all times.</p>
 * <p>It works by mapping "chunks" of the stream into memory, than reading from it.
 * This class provides all the machinery to read from chunks;
 * subclasses need only implement the abstract method {@link #loadChunk(long, long)}.</p>
 */
public abstract class SeekableBufferedInputStream extends SeekableInputStream {

  /**
   * The default chunk size in bytes: 64 Kb.
   */
  protected static int DEFAULT_CHUNK_SIZE = 8192;

  /**
   * The chunk size.
   */
  private final long chunkSize;

  /**
   * The chunk's bit mask. Used to infer the position relative to the current mapped chunk
   * from the stream's (global) position.
   */
  private final long chunkMask;

  /**
   * Used to infer the chunk index that contains the stream's position to be read.
   */
  private final int chunkShift;

  /**
   * The current chunk.
   */
  private ByteBuffer chunk;

  /**
   * The zero-based index of the current mapped chunk.
   */
  private int chunkIndex;

  /**
   * Creates a SeekableBufferedInputStream with a default chunk size.
   * @param length The stream's total length in bytes
   */
  public SeekableBufferedInputStream(long length) {
    this(length, DEFAULT_CHUNK_SIZE);
  }

  /**
   * Creates a SeekableBufferedInputStream with the specified chunk size.
   * The chunk size <em>must</em> be a power of 2.
   * @param length The stream's total length in bytes
   * @param chunkSize The chunk size in bytes; <em>must</em> be a power of 2
   */
  public SeekableBufferedInputStream(long length, long chunkSize) {
    super(length);
    if (Long.bitCount(chunkSize) != 1) {
      throw new IllegalArgumentException("Chunk size must be a power of 2: " + chunkSize);
    }
    this.chunkSize = chunkSize;
    this.chunkMask = chunkSize - 1;
    this.chunkShift = Long.numberOfTrailingZeros(chunkSize);
    this.chunk = null;
    this.chunkIndex = -1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() throws IOException {
    if (remaining() <= 0) {
      return -1;
    }
    long position = tell();
    ensureChunk(position);
    chunk.position((int) (position & chunkMask));
    int b = chunk.get() & 0xff;
    seek(position + 1);
    return b;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (remaining() <= 0) {
      return -1;
    }
    // adjust length if needed, so that exactly len bytes will be read
    len = (int) Math.min((long) Math.min(len, b.length - off), remaining());
    fillBuffer(b, off, len);
    // increment position by the number of bytes read
    seek(tell() + len);
    // return the number of bytes read
    return len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    super.close();
    // best effort to signal that the chunk can be safely deallocated
    chunk = null;
  }

  /**
   * Fills the given byte buffer with bytes read from the underlying stream.
   * @param      b     the buffer into which the data is read.
   * @param      off   the start offset in array <code>b</code>
   *                   at which the data is written.
   * @param      len   the maximum number of bytes to read.
   * @throws IOException
   */
  private void fillBuffer(byte[] b, int off, int len) throws IOException {
    long pos = tell();
    while (len > 0) {
      // ensure the the right chunk is loaded
      ensureChunk(pos);
      // position the chunk to the desired relative offset
      chunk.position((int) (pos & chunkMask));
      // make the actual copy
      int copied = Math.min(len, chunk.remaining());
      chunk.get(b, off, copied);
      // increment counters
      pos += copied;
      off += copied;
      len -= copied;
    }
  }

  /**
   * Ensure that the right chunk is loaded in memory.
   * @param position the stream position to read, used to locate the chunk containing it.
   * @throws IOException
   */
  private void ensureChunk(long position) throws IOException {
    // the chunk index that will contain the bytes we want to read
    int chunkIndex = (int) (position >>> chunkShift);
    // if there is no current chunk or the current chunk is not the good one
    // we need to swap chunks
    if (chunk == null || chunkIndex != this.chunkIndex) {
      // compute start and length for the chunk to be loaded
      long start = chunkIndex * chunkSize;
      long length = Math.min(chunkSize, length() - start);
      // load the new chunk
      this.chunk = loadChunk(start, length);
      this.chunkIndex = chunkIndex;
    }
  }

  /**
   * <p>Return a {@link ByteBuffer} instance representing a "chunk" of the underlying stream.</p>
   *
   * <p>The returned {@link ByteBuffer} is required to have its position set to 0 and
   * its limit set to <code>length</code>.</p>
   *
   * <p>The {@link ByteBuffer} would typically be allocated "off-heap", although this
   * is not a requirement.</p>
   *
   * @param start the stream's starting position
   * @param length the number of bytes to load
   * @return {@link ByteBuffer} instance representing a "chunk" of the underlying stream.
   * @throws IOException
   */
  protected abstract ByteBuffer loadChunk(long start, long length) throws IOException;

}
