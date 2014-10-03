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

package org.apache.flume.sink.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * Unit tests that exercise HDFSEventSink on an actual instance of HDFS.
 * TODO: figure out how to unit-test Kerberos-secured HDFS.
 */
public class IntegrationTestHDFSEventSink {

  private static final Logger logger =
      LoggerFactory.getLogger(IntegrationTestHDFSEventSink.class);

  private static final boolean KEEP_DATA = false;
  private static final String DFS_DIR = "target/test/dfs";
  private static final String TEST_BUILD_DATA_KEY = "test.build.data";
  private static final String HDFS_BASE_DIR = "/flume-tests";
  private static final Path HDFS_BASE_PATH = new Path(HDFS_BASE_DIR);

  private static String oldTestBuildDataProp = null;
  private Configuration configuration = null;
  private FileSystem fs = null;
  private static String nameNodeURL = null;

  @BeforeClass
  public static void isHDFSRunning() throws IOException {
    nameNodeURL = System.getProperty("hdfs.namenode.url");
    if (nameNodeURL == null) {
      nameNodeURL = "hdfs://localhost:50070";
    }

    final FileSystem fs = FileSystem.get(URI.create(nameNodeURL), new Configuration());
    try {
      fs.exists(new Path(nameNodeURL + "/"));
    } catch (IOException ex) {
      logger.warn("Cannot run HDFS integration tests", ex);
      Assume.assumeTrue(false);
    }
  }

  @Before
  public void setup() throws IOException {
    configuration = new Configuration();
    configuration.set("fs.default.name", nameNodeURL);
    fs = FileSystem.get(URI.create(nameNodeURL), configuration);
  }

  @After
  public void tearDown() throws IOException {
    if (!KEEP_DATA) {
      if (fs.exists(HDFS_BASE_PATH)) {
        fs.delete(HDFS_BASE_PATH, true);
      }
      FileUtils.deleteQuietly(new File(DFS_DIR));
    }

    fs.close();
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Test
  public void simpleHDFSTest() throws EventDeliveryException, IOException {
    Path outputDirPath = new Path(HDFS_BASE_PATH, "/simpleHDFSTest");

    logger.info("Running test with output dir: {}", outputDirPath);

    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    logger.info("Namenode address: {}", nameNodeURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nameNodeURL + outputDirPath.toUri().toString());
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    String EVENT_BODY = "yarg!";
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody(EVENT_BODY, Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store event to HDFS
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);
    Assert.assertEquals("Only one file expected", 1, statuses.length);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertEquals(EVENT_BODY, line);
    }

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }
  }

  /**
   * Writes two events in GZIP-compressed serialize.
   */
  @Test
  public void simpleHDFSGZipCompressedTest() throws EventDeliveryException, IOException {
    Path outputDirPath = new Path(HDFS_BASE_PATH, "/simpleHDFSGZipCompressedTest");

    logger.info("Running test with output dir: {}", outputDirPath);

    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    logger.info("Namenode address: {}", nameNodeURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nameNodeURL + outputDirPath.toUri().toString());
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.CompStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));
    sinkCtx.put("hdfs.codeC", "gzip");

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    String EVENT_BODY_1 = "yarg1";
    String EVENT_BODY_2 = "yarg2";
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody(EVENT_BODY_1, Charsets.UTF_8));
      channel.put(EventBuilder.withBody(EVENT_BODY_2, Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store event to HDFS
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);
    Assert.assertEquals("Only one file expected", 1, statuses.length);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          new GZIPInputStream(stream)));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertEquals(EVENT_BODY_1, line);

      // The rest of this test is commented-out (will fail) for 2 reasons:
      //
      // (1) At the time of this writing, Hadoop has a bug which causes the
      // non-native gzip implementation to create invalid gzip files when
      // finish() and resetState() are called. See HADOOP-8522.
      //
      // (2) Even if HADOOP-8522 is fixed, the JDK GZipInputStream is unable
      // to read multi-member (concatenated) gzip files. See this Sun bug:
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4691425
      //
      //line = reader.readLine();
      //logger.info("Second line in file {}: {}", filePath, line);
      //Assert.assertEquals(EVENT_BODY_2, line);
    }

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Test
  @Ignore("We are not doing integration tests with replicated HDFS by default")
  public void underReplicationTest() throws EventDeliveryException,
      IOException {
    Path outputDirPath = new Path(HDFS_BASE_PATH, "/underReplicationTest");

    logger.info("Running test with output dir: {}", outputDirPath);

    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    logger.info("Namenode address: {}", nameNodeURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nameNodeURL + outputDirPath.toUri().toString());
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody("yarg 1", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 2", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 3", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 4", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 5", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 5", Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store events to HDFS
    logger.info("Running process(). Create new file.");
    sink.process(); // create new file;
    logger.info("Running process(). Same file.");
    sink.process();

    // kill a datanode
    logger.info("Killing datanode #1...");
    //TODO: Kill data node #1
    //FIXME

    // there is a race here.. the client may or may not notice that the
    // datanode is dead before it next sync()s.
    // so, this next call may or may not roll a new file.

    logger.info("Running process(). Create new file? (racy)");
    sink.process();

    logger.info("Running process(). Create new file.");
    sink.process();

    logger.info("Running process(). Create new file.");
    sink.process();

    logger.info("Running process(). Create new file.");
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertTrue(line.startsWith("yarg"));
    }

    Assert.assertTrue("4 or 5 files expected, found " + statuses.length,
        statuses.length == 4 || statuses.length == 5);
    System.out.println("There are " + statuses.length + " files.");

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Ignore("This test is flakey and causes tests to fail pretty often.")
  @Test
  public void maxUnderReplicationTest() throws EventDeliveryException,
      IOException {
    Path outputDirPath = new Path(HDFS_BASE_PATH, "/underReplicationTest");

    logger.info("Running test with output dir: {}", outputDirPath);

    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    logger.info("Namenode address: {}", nameNodeURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nameNodeURL + outputDirPath.toUri().toString());
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    channel.getTransaction().begin();
    try {
      for (int i = 0; i < 50; i++) {
        channel.put(EventBuilder.withBody("yarg " + i, Charsets.UTF_8));
      }
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store events to HDFS
    logger.info("Running process(). Create new file.");
    sink.process(); // create new file;
    logger.info("Running process(). Same file.");
    sink.process();

    // kill a datanode
    logger.info("Killing datanode #1...");
    //TODO: Kill data node #1
    //FIXME

    // there is a race here.. the client may or may not notice that the
    // datanode is dead before it next sync()s.
    // so, this next call may or may not roll a new file.

    logger.info("Running process(). Create new file? (racy)");
    sink.process();

    for (int i = 3; i < 50; i++) {
      logger.info("Running process().");
      sink.process();
    }

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertTrue(line.startsWith("yarg"));
    }

    System.out.println("There are " + statuses.length + " files.");
    Assert.assertEquals("31 files expected, found " + statuses.length,
        31, statuses.length);

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }
  }

}
