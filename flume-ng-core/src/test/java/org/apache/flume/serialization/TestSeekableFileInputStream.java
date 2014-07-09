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

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

public class TestSeekableFileInputStream extends TestSeekableInputStream<SeekableFileInputStream> {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File file;

  @Override
  @Test
  public void testMarkReadAheadLimit() throws IOException {
    super.testMarkReadAheadLimit();
    // special corner case where an IOE would be wrapped in a RuntimeException
    in.close();
    try {
      in.mark(1);
      fail("Should have thrown a Runtime exception");
    } catch(RuntimeException e) {}
  }

  @Test(expected = NullPointerException.class)
  public void testNullFile() throws IOException {
    new SeekableFileInputStream((String) null);
  }

  @Override
  protected SeekableFileInputStream newInputStream(byte[] contents) throws IOException {
    createFile(contents);
    return new SeekableFileInputStream(file.getAbsolutePath());
  }

  private void createFile(byte[] contents) throws IOException {
    file = temporaryFolder.newFile();
    FileUtils.writeByteArrayToFile(file, contents);
  }

}
