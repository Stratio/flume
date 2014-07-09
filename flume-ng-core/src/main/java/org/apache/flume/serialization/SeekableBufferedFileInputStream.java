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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A {@link Seekable}, buffered {@link InputStream} that reads from a file.
 * The file is mapped into {@link ByteBuffer} "chunks" of fixed size, allocated off-heap.
 */
public class SeekableBufferedFileInputStream extends SeekableBufferedInputStream {

  private final RandomAccessFile randomAccessFile;

  private final FileChannel fileChannel;

  /**
   * Constructs a new SeekableBufferedFileInputStream with a default chunk size.
   * @param file the file to read from
   * @throws FileNotFoundException
   */
  public SeekableBufferedFileInputStream(File file) throws FileNotFoundException {
    this(file, DEFAULT_CHUNK_SIZE);
  }

  /**
   * Constructs a new SeekableBufferedFileInputStream with the specified chunk size.
   * @param file the file to read from
   * @param chunkSize the chunk size, in bytes
   * @throws FileNotFoundException
   */
  public SeekableBufferedFileInputStream(File file, long chunkSize) throws FileNotFoundException {
    super(file.length(), chunkSize);
    this.randomAccessFile = new RandomAccessFile(file, "r");
    this.fileChannel = this.randomAccessFile.getChannel();
  }

  /**
   * {@inheritDoc}
   *
   * This implementation uses a {@link FileChannel} to map portions of the file into a {@link ByteBuffer}.
   */
  @Override
  protected ByteBuffer loadChunk(long start, long length) throws IOException {
    return fileChannel.map(FileChannel.MapMode.READ_ONLY, start, length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    super.close();
    randomAccessFile.close();
    fileChannel.close();
  }

}
