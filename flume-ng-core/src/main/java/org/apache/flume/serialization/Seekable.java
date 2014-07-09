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

import java.io.Closeable;
import java.io.IOException;

/**
 * <p>A {@link Resettable} stream that supports the following main features:</p>
 *
 * <ol>
 *   <li>Seek: ability to jump to any arbitrary position (see {@link #seek(long)}):</li>
 *   <li>Tell: ability to report its current position (see {@link #tell()});</p></li>
 *   <li>Mark: ability to mark any arbitrary position (see {@link #mark(long)});</li>
 *   <li>Point: ability to report its current mark (see {@link #point()});</li>
 * </ol>
 *
 * <p>Additionally, a Seekable object is also able to:</p>
 *
 * <ol>
 *   <li>Report its length (see {@link #length()});</li>
 *   <li>Report the number of units remaining to be read from its current position to the
 * end of the stream (see {@link #remaining()}).</li>
 * </ol>
 */
public interface Seekable extends Resettable, Closeable {

  /**
   * Jumps to the specified position. The stream's position
   * indicates which unit (byte or char) will be read by the next
   * read operation.
   * Valid positions are comprised between 0 (beginning of the stream)
   * and {@link #length()} inclusive.
   * Note that a position equal to {@link #length()} is valid,
   * and indicates that the stream is at its end
   * (i.e. the next read operation will return -1).
   * Attempts to seek an invalid position <em>should</em>
   * result in an exception being thrown, although
   * implementations might choose to raise an exception
   * at the next call to a read operation instead.
   *
   * @param position the position to jump to.
   * @throws IOException
   */
  void seek(long position) throws IOException;

  /**
   * Returns the stream's current position. The stream's position
   * indicates which unit (byte or char) will be read by the next
   * read operation.
   * @return the stream's current position.
   * @throws IOException
   */
  long tell() throws IOException;

  /**
   * Marks the given position, i.e., indicates that the
   * specified position should be returned to in the case of
   * {@link #reset()} being called.
   * Valid positions are comprised between 0 (beginning of the stream)
   * and {@link #length()} inclusive.
   * Note that a position equal to {@link #length()} is valid,
   * and indicates that the stream is at its end
   * (i.e. the next read operation will return -1).
   * Attempts to mark an invalid position <em>should</em>
   * result in an exception being thrown, although
   * implementations might choose to raise an exception
   * at the next call to {@link #reset()} or to a read operation instead.
   *
   * @param position The position to be returned to in the case of
   * {@link #reset()} being called.
   *
   * @throws java.io.IOException
   */
  void mark(long position) throws IOException;

  /**
   * Returns the last saved mark position, without changing the stream's current position.
   * Should return 0 (beginning of file) if no mark position has been explicitly
   * set yet for this stream, in particular by calling either {@link #mark()} or {@link #mark(long)}.
   * @throws IOException
   */
  long point() throws IOException;

  /**
   * Returns the total length of the stream.
   * @return the stream's total length.
   * @throws IOException
   */
  long length() throws IOException;

  /**
   * Returns the number of units remaining to be read
   * from the stream's current position to its end.
   * The number of remaining units should be equal to
   * {@link #length()} - {@link #tell()};
   * @return the number of units remaining to be read
   * from the stream's current position to its end.
   * @throws IOException
   */
  long remaining() throws IOException;

}