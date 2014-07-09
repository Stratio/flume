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

import java.io.ByteArrayInputStream;

/**
 * A simple subclass of ByteArrayInputStream that implements Seekable.
 * Intended for unit tests only.
 */
public class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable {

  public SeekableByteArrayInputStream(byte[] bytes) {
    super(bytes);
    mark = 0;
  }

  @Override
  public synchronized void reset() {
    seek(mark);
  }

  @Override
  public void seek(long pos) {
    if (pos < 0) throw new ArrayIndexOutOfBoundsException((int) pos);
    if (pos > count) throw new ArrayIndexOutOfBoundsException((int) pos);
    this.pos = (int) pos;
  }

  @Override
  public long tell() {
    return pos;
  }

  @Override
  public long length() {
    return buf.length;
  }

  @Override
  public long remaining() {
    return available();
  }

  @Override
  public void mark() {
    this.mark = pos;
  }

  @Override
  public void mark(long position) {
    this.mark = (int) position;
  }

  @Override
  public long point() {
    return mark;
  }
}
