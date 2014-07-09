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

import org.fest.reflect.field.Invoker;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.fest.reflect.core.Reflection.field;
import static org.junit.Assert.*;

public abstract class TestSeekableBufferedInputStream<T extends SeekableBufferedInputStream> extends TestSeekableInputStream<T>{

  @Test
  public void testReadByte() throws IOException {
    for(int expected = 0; expected < CONTENTS.length; expected++) {
      assertEquals(codePointAt(expected), in.read());
    }
    assertEquals(-1, in.read());
    assertEquals(-1, in.read());
  }

  @Test
  public void testReadByteArray() throws IOException {
    byte[] buf = new byte[3];
    // write 30 bytes from offset 2 -> will actually write 1 byte only
    int read = in.read(buf, 2, 30);
    assertEquals(1, read);
    assertEquals(CONTENTS[0], buf[2]);
    // write 20 bytes from offset 0 -> will actually write 2 bytes only
    read = in.read(buf, 0, 2);
    assertEquals(2, read);
    assertEquals(CONTENTS[1], buf[0]);
    assertEquals(CONTENTS[2], buf[1]);
    consumeFully(in);
    // write 3 bytes from offset 0 -> will actually not write any bytes
    read = in.read(buf, 0, 3);
    assertEquals(-1, read);
  }

  @Test
  public void testClose() throws IOException {
    in.close();
    ByteBuffer chunk = field("chunk").ofType(ByteBuffer.class).in(in).get();
    assertNull(chunk);
  }

  @Test
  public void testLoadChunk() throws IOException {
    int offset = 3;
    int length = 10;
    ByteBuffer chunk = in.loadChunk(offset, length);
    assertEquals(0, chunk.position());
    assertEquals(length, chunk.limit());
    for(int i = 0; i < length; i++) {
      assertEquals(CONTENTS[i + offset], chunk.get(i));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongChunkSize() throws IOException {
    in = newInputStream(CONTENTS, 15);
  }

  @Test
  public void testReadMultipleChunks() throws IOException {
    in =  newInputStream(CONTENTS, 4);
    Invoker<Integer> chunkIndex = field("chunkIndex").ofType(Integer.TYPE).in(in);
    assertEquals(-1, (int) chunkIndex.get());
    in.read(); // loads chunk 0
    assertEquals(0, (int) chunkIndex.get());
    in.seek(3);
    in.read(); // still chunk 0
    in.mark(); // will mark position 4 -> chunk 1
    assertEquals(0, (int) chunkIndex.get());
    in.seek(4);
    in.read(); // loads chunk 1
    assertEquals(1, (int) chunkIndex.get());
    in.seek(11);
    in.read(); // loads chunk 2
    assertEquals(2, (int) chunkIndex.get());
    consumeFully(in); // loads chunk 3
    assertEquals(3, (int) chunkIndex.get());
    in.reset();
    in.read();// reloads chunk 1
    assertEquals(1, (int) chunkIndex.get());
  }

  protected abstract T newInputStream(byte[] contents, int chunkSize) throws IOException;

}
