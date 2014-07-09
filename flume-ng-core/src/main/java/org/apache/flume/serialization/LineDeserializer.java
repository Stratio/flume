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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * A deserializer that parses text lines from a supplied {@link Seekable} {@link InputStream}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LineDeserializer implements EventDeserializer {

  /**
   * The character stream to read lines from.
   */
  private final ResettableLineReader in;

  private final Charset outputCharset;
  private final int maxLineLength;
  private volatile boolean isOpen;

  /** Character set used when reading the input. */
  public static final String INPUT_CHARSET_KEY = "inputCharset";
  public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

  /** Character set used when creating new Events. */
  public static final String OUTPUT_CHARSET_KEY = "outputCharset";
  public static final String DEFAULT_OUTPUT_CHARSET = "UTF-8";

  /** What to do when there is a character set decoding error. */
  public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
  public static final String DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL.name();

  public static final String MAXLINE_KEY = "maxLineLength";
  public static final int MAXLINE_DFLT = 2048;

  LineDeserializer(Context context, InputStream in) throws IOException {
    this.outputCharset = Charset.forName(
        context.getString(OUTPUT_CHARSET_KEY, DEFAULT_OUTPUT_CHARSET));
    Charset inputCharset = Charset.forName(
        context.getString(INPUT_CHARSET_KEY, DEFAULT_INPUT_CHARSET));
    Preconditions.checkNotNull(inputCharset);
    DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
        context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
            .toUpperCase());
    Preconditions.checkNotNull(decodeErrorPolicy);
    CharsetDecoder decoder = decodeErrorPolicy.newDecoder(inputCharset);
    this.in = new ResettableLineReader(new ResettableInputStreamReader(in, decoder));
    this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
    this.isOpen = true;
  }

  /**
   * Reads a line from a file and returns an event
   * @return Event containing parsed line
   * @throws IOException
   */
  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    String line = in.readLine(maxLineLength);
    if (line == null) {
      return null;
    } else {
      return EventBuilder.withBody(line, outputCharset);
    }
  }

  /**
   * Batch line read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read lines
   * @throws IOException
   */
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, InputStream in) {
      if (!(in instanceof Seekable)) {
        throw new IllegalArgumentException(
            "Cannot use this deserializer without a Seekable input stream");
      }
      try {
        return new LineDeserializer(context, in);
      } catch (IOException e) {
        throw new FlumeException("Cannot instantiate deserializer", e);
      }
    }

  }

}
