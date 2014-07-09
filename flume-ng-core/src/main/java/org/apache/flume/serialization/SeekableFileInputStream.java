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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

/**
 * A non-buffered version of a {@link Seekable} {@link InputStream} that reads from a file.
 * Can be used as a drop-in replacement for a {@link FileInputStream} where a {@link Seekable} is required.
 * However, since it is not buffered, it performs slightly worse than {@link SeekableBufferedFileInputStream},
 * which should be the preferred implementation for most cases.
 */
public class SeekableFileInputStream extends FilterInputStream implements Seekable {

  private final long length;

  private long mark;

  /**
   * Definition borrowed from  {@link FileInputStream}:
   *
   * Creates a <code>FileInputStream</code> by
   * opening a connection to an actual file,
   * the file named by the path name <code>name</code>
   * in the file system.  A new <code>FileDescriptor</code>
   * object is created to represent this file
   * connection.
   * <p/>
   * First, if there is a security
   * manager, its <code>checkRead</code> method
   * is called with the <code>name</code> argument
   * as its argument.
   * <p/>
   * If the named file does not exist, is a directory rather than a regular
   * file, or for some other reason cannot be opened for reading then a
   * <code>FileNotFoundException</code> is thrown.
   *
   * @param name the system-dependent file name.
   * @throws java.io.FileNotFoundException if the file does not exist,
   *                                       is a directory rather than a regular file,
   *                                       or for some other reason cannot be opened for
   *                                       reading.
   * @throws SecurityException             if a security manager exists and its
   *                                       <code>checkRead</code> method denies read access
   *                                       to the file.
   * @see java.lang.SecurityManager#checkRead(java.lang.String)
   */
  public SeekableFileInputStream(String name) throws FileNotFoundException {
    this(name != null ? new File(name) : null);
  }

  /**
   * Definition borrowed from  {@link FileInputStream}:
   *
   * Creates a <code>FileInputStream</code> by
   * opening a connection to an actual file,
   * the file named by the <code>File</code>
   * object <code>file</code> in the file system.
   * A new <code>FileDescriptor</code> object
   * is created to represent this file connection.
   * <p/>
   * First, if there is a security manager,
   * its <code>checkRead</code> method  is called
   * with the path represented by the <code>file</code>
   * argument as its argument.
   * <p/>
   * If the named file does not exist, is a directory rather than a regular
   * file, or for some other reason cannot be opened for reading then a
   * <code>FileNotFoundException</code> is thrown.
   *
   * @param file the file to be opened for reading.
   * @throws java.io.FileNotFoundException if the file does not exist,
   *                                       is a directory rather than a regular file,
   *                                       or for some other reason cannot be opened for
   *                                       reading.
   * @throws SecurityException             if a security manager exists and its
   *                                       <code>checkRead</code> method denies read access to the file.
   * @see java.io.File#getPath()
   * @see java.lang.SecurityManager#checkRead(java.lang.String)
   */
  public SeekableFileInputStream(File file) throws FileNotFoundException {
    super(new FileInputStream(file));
    this.length = file.length();
    this.mark = 0; // mark the beginning of file
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) throw new IOException("Invalid position: " + pos);
    if (pos > length()) throw new IOException("Invalid position: " + pos);
    getChannel().position(pos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long tell() throws IOException {
    return getChannel().position();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark() throws IOException {
    this.mark = tell();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark(int readlimit) {
    try {
      this.mark = getChannel().position();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mark(long position) throws IOException {
    this.mark = position;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long point() throws IOException {
    return mark;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    if (mark < 0) throw new IOException("Resetting to invalid mark");
    seek(mark);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long remaining() throws IOException {
    return length() - tell();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long length() throws IOException {
    return length;
  }

  /**
   * Definition borrowed from  {@link FileInputStream}:
   *
   * Returns the unique {@link java.nio.channels.FileChannel FileChannel}
   * object associated with this file input stream.
   * <p/>
   * <p> The initial {@link java.nio.channels.FileChannel#position()
   * </code>position<code>} of the returned channel will be equal to the
   * number of bytes read from the file so far.  Reading bytes from this
   * stream will increment the channel's position.  Changing the channel's
   * position, either explicitly or by reading, will change this stream's
   * file position.
   *
   * @return the file channel associated with this file input stream
   */
  public FileChannel getChannel() {
    return ((FileInputStream) in).getChannel();
  }
}
