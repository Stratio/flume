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

import com.google.common.base.Throwables;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link Seekable} {@link InputStream} that has the ability of persisting its
 * current position to a supplied {@link PositionTracker}.
 *
 * The underlying {@link InputStream} is required to also implement {@link Seekable}.
 *
 */
public class SeekableReliableInputStream extends FilterInputStream implements Seekable {

  private final Seekable seekable;

  private final PositionTracker tracker;

  /**
   *
   * @param in
   *        {@link InputStream} instance to read bytes from. Must implement {@link Seekable},
   *        otherwise an {@link IllegalArgumentException} is thrown.
   *
   * @param tracker
   *        {@link PositionTracker} instance to use.
   *
   * @throws IOException
   *         If an I/O error occurs while synchronizing the supplied {@link InputStream}
   *         with the supplied {@link PositionTracker}.
   *
   * @throws IllegalArgumentException
   *         If the supplied {@link InputStream} does not implement {@link Seekable}
   */
  public SeekableReliableInputStream(InputStream in, PositionTracker tracker) throws IOException {
    super(in);
    if( ! (in instanceof Seekable)) {
      throw new IllegalArgumentException("This InputStream requires that the underlying InputStream implement Seekable");
    }
    this.seekable = (Seekable) in;
    this.tracker = tracker;
    // synchronize stream and tracker
    this.seekable.seek(tracker.getPosition());
  }

  // InputStream overridden methods

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void mark(int readlimit) {
    try {
      mark();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    super.close();
    tracker.close();
  }

  // Resettable interface

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void mark() throws IOException {
    tracker.storePosition(tell());
    seekable.mark();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark(long position) throws IOException {
    tracker.storePosition(position);
    seekable.mark(position);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long point() {
    return tracker.getPosition();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    seek(tracker.getPosition());
  }

  // Seekable interface

  /**
   * {@inheritDoc}
   */
  @Override
  public void seek(long pos) throws IOException {
    seekable.seek(pos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long tell() throws IOException {
    return seekable.tell();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long length() throws IOException {
    return seekable.length();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long remaining() throws IOException {
    return seekable.remaining();
  }

}
