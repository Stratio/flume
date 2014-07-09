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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;

/**
 * <p>A {@link Resettable} {@link Reader} that has the ability to read entire
 * lines of text in a single read operation.</p>
 *
 * <p>Implementing {@link Resettable} makes it impractical to perform buffering at this level,
 * so this class does not perform any buffering at all, and can be thought of
 * as a non-buffered version of the widely-used {@link java.io.BufferedReader}.</p>
 */
public class ResettableLineReader extends FilterReader implements Resettable {

  private static final Logger logger = LoggerFactory.getLogger(ResettableLineReader.class);

  private final Resettable resettable;

  /**
   * Indicates if the last read char was a carriage return.
   */
  private boolean cr;

  /**
   * Indicates that the last line was truncated.
   */
  private boolean truncated;

  /**
   * Creates a new ResettableLineReader.
   * @param in a {@link Reader} object providing the underlying stream.
   * @throws NullPointerException if <code>in</code> is <code>null</code>
   */
  public <T extends Reader & Resettable> ResettableLineReader(T in) {
    super(in);
    this.resettable = in;
    this.truncated = false;
    this.cr = false;
  }

  /**
   * Definition borrowed from {@link java.io.BufferedReader}:
   *
   * Reads a line of text. A line is considered to be terminated by any one
   * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
   * followed immediately by a linefeed.
   *
   * @return A String containing the contents of the line, not including
   *         any line-termination characters, or null if the end of the
   *         stream has been reached
   *
   * @throws java.io.IOException If an I/O error occurs
   */
  public String readLine() throws IOException {
    return readLine(-1);
  }

  /**
   * Reads a line of text. A line is considered to be terminated by any one
   * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
   * followed immediately by a linefeed.
   * If <code>maxLineLength</code> is greater than zero,
   * and the line length exceeds that value, then the line is truncated.
   * When a line of text is truncated, its remaining chars
   * will be returned as separate lines,
   * at subsequent calls to {@link #readLine()}.
   *
   * @param maxLineLength the maximum line length allowed; if the line length
   *                      exceeds this value, then it is truncated. If zero or
   *                      negative, lines are never truncated.
   *
   * @return A String containing the contents of the line, not including
   *         any line-termination characters, or null if the end of the
   *         stream has been reached
   *
   * @throws java.io.IOException If an I/O error occurs
   *
   * @see #readLine()
   */
  public String readLine(int maxLineLength) throws IOException {
    StringBuilder sb = null;
    int readChars = 0;
    int c;
    while ((c = in.read()) != -1) {
      boolean lf = c == '\n';
      if(cr && lf) {
        // this LF is following a CR: just ignore it
        continue;
      }
      cr = c == '\r';
      boolean eol = cr || lf;
      if(truncated && eol) {
        // any line terminator following a truncation should be ignored
        continue;
      };
      if(sb == null) {
        sb = new StringBuilder();
      }
      // terminate the current line
      if (eol) {
        break;
      }
      readChars++;
      sb.append((char) c);
      truncated = maxLineLength > 0 && readChars == maxLineLength;
      if (truncated) {
        logger.warn("Line length exceeds max ({}), truncating line!", maxLineLength);
        break;
      }
    }
    if (sb != null) {
      // as long as sb is not null we have a (potentially empty) line
      return sb.toString();
    } else {
      // end of file
      return null;
    }
  }

  /**
   *{@inheritDoc}
   */
  @Override
  public void mark() throws IOException {
    resettable.mark();
  }

}