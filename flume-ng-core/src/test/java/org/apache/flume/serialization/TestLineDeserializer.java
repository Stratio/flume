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
import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class TestLineDeserializer {

  private String mini;

  @Before
  public void setup() {
    StringBuilder sb = new StringBuilder();
    sb.append("line 1\n");
    sb.append("line 2\n");
    mini = sb.toString();
  }

  @Test
  public void testSimple() throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    EventDeserializer des = new LineDeserializer(new Context(), in);
    validateMiniParse(des, LineDeserializer.DEFAULT_OUTPUT_CHARSET);
  }

  @Test
  public void testSimpleViaBuilder() throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    EventDeserializer.Builder builder = new LineDeserializer.Builder();
    EventDeserializer des = builder.build(new Context(), in, new TransientPositionTracker(""));
    validateMiniParse(des, LineDeserializer.DEFAULT_OUTPUT_CHARSET);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testViaBuilderWithNotSeekable() throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    EventDeserializer.Builder builder = new LineDeserializer.Builder();
    builder.build(new Context(), in,  new TransientPositionTracker(""));
  }

  @Test
  public void testSimpleViaFactory() throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    EventDeserializer des;
    des = EventDeserializerFactory.getInstance("LINE", new Context(), in,  new TransientPositionTracker(""));
    validateMiniParse(des, LineDeserializer.DEFAULT_OUTPUT_CHARSET);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testViaFactoryWithNotSeekable() throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    EventDeserializerFactory.getInstance("LINE", new Context(), in,  new TransientPositionTracker(""));
  }

  @Test
  public void testBatch() throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    EventDeserializer des = new LineDeserializer(new Context(), in);
    List<Event> events;

    events = des.readEvents(1); // only try to read 1
    Assert.assertEquals(1, events.size());
    assertEventBodyEquals("line 1", events.get(0), LineDeserializer.DEFAULT_OUTPUT_CHARSET);

    events = des.readEvents(10); // try to read more than we should have
    Assert.assertEquals(1, events.size());
    assertEventBodyEquals("line 2", events.get(0), LineDeserializer.DEFAULT_OUTPUT_CHARSET);

    des.mark();
    des.close();
  }

  // truncation occurs at maxLineLength boundaries
  @Test
  public void testMaxLineLength() throws IOException {
    String longLine = "abcdefghijklmnopqrstuvwxyz\n";
    Context ctx = new Context();
    ctx.put(LineDeserializer.MAXLINE_KEY, "10");

    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(longLine.getBytes(Charsets.UTF_8));
    EventDeserializer des = new LineDeserializer(ctx, in);

    assertEventBodyEquals("abcdefghij", des.readEvent(), LineDeserializer.DEFAULT_OUTPUT_CHARSET);
    assertEventBodyEquals("klmnopqrst", des.readEvent(), LineDeserializer.DEFAULT_OUTPUT_CHARSET);
    assertEventBodyEquals("uvwxyz", des.readEvent(), LineDeserializer.DEFAULT_OUTPUT_CHARSET);
    Assert.assertNull(des.readEvent());
  }

  @Test
  public void testAlternateInputCharset() throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(mini.getBytes(Charsets.UTF_16));
    Context context = new Context();
    context.put(LineDeserializer.INPUT_CHARSET_KEY, "UTF-16");
    EventDeserializer des = new LineDeserializer(context, in);
    validateMiniParse(des, LineDeserializer.DEFAULT_OUTPUT_CHARSET);
  }

  @Test
  public void testAlternateOutputCharset() throws IOException {
    SeekableByteArrayInputStream in = new SeekableByteArrayInputStream(mini.getBytes(Charsets.UTF_8));
    Context context = new Context();
    context.put(LineDeserializer.OUTPUT_CHARSET_KEY, "ISO-8859-1");
    EventDeserializer des = new LineDeserializer(context, in);
    validateMiniParse(des, "ISO-8859-1");
  }

  private void assertEventBodyEquals(String expected, Event event, String charset) throws IOException {
    String bodyStr = new String(event.getBody(), charset);
    Assert.assertEquals(expected, bodyStr);
  }

  private void validateMiniParse(EventDeserializer des, String charset) throws IOException {
    Event evt;

    evt = des.readEvent();
    Assert.assertEquals("line 1", new String(evt.getBody(), charset));
    des.mark();

    evt = des.readEvent();
    Assert.assertEquals("line 2", new String(evt.getBody(), charset));
    des.reset(); // reset!

    evt = des.readEvent();
    Assert.assertEquals("Line 2 should be repeated, " +
        "because we reset() the stream", new String(evt.getBody(), charset), "line 2");

    evt = des.readEvent();
    Assert.assertNull("Event should be null because there are no lines " +
        "left to read", evt);

    des.mark();
    des.close();
  }
}
