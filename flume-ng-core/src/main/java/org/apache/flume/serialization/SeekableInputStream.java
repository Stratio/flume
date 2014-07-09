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
import java.io.InputStream;

/**
 * <p>A base class for {@link InputStream}s that implement {@link Seekable}.</p>
 *
 * <p>This class provides basic implementations for all methods of the {@link Seekable} interface,
 * but leaves for subclasses the task of actually implementing read methods.</p>
 *
 * <p>Implementation note: this class does <em>not</em> synchronize on any monitor,
 * and thus assumes that it is not modified by more than one thread concurrently.</p>
 */
public abstract class SeekableInputStream extends InputStream implements Seekable {

  private final long length;

  private long position;

  private long remaining;

  private long mark;

  /**
   * @param length The stream's total length in bytes
   */
  public SeekableInputStream(long length) {
    this.length = length;
    this.remaining = length;
    this.position = 0;
    this.mark = 0;
  }

  // InputStream overridden methods

  /**
   * {@inheritDoc}
   *
   * <p>Note: this implementation assumes that the stream never blocks
   * and thus simply returns the number of bytes remaining (see {@link #remaining()}),
   * or {@link Integer#MAX_VALUE}, if the number of remaining bytes exceed that value.</p>
   */
  @Override
  public int available() throws IOException {
    return (int) Math.min((long) Integer.MAX_VALUE, remaining());
  }

  /**
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
   * this implementation simply ignores the <code>readAheadLimit</code> parameter,
   * so that this method behaves exactly like {@link #mark()}.</p>
   *
   * @see #mark()
   */
  @Override
  public void mark(int readlimit) {
    mark = position;
  }

  // Resettable interface

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark() throws IOException {
    mark = position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark(long position) throws IOException {
    mark = position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long point() {
    return mark;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    seek(mark);
  }

  // Seekable interface

  /**
   * {@inheritDoc}
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) throw new IOException("Invalid position: " + pos);
    if (pos > length) throw new IOException("Invalid position: " + pos);
    position = pos;
    remaining = length - position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long tell() throws IOException {
    return position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long length() throws IOException {
    return length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long remaining() throws IOException {
    return remaining;
  }

}