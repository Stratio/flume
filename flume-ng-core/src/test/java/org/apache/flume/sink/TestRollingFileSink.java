/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleException;
import org.fest.reflect.field.Invoker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.*;
import static org.fest.reflect.core.Reflection.*;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestRollingFileSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestRollingFileSink.class);

  private File tmpDir;
  private RollingFileSink sink;

  @Before
  public void setUp() throws IOException {
    tmpDir = Files.createTempDir();
    sink = new RollingFileSink();
    sink.setChannel(new MemoryChannel());
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    Context context = new Context();

    context.put("sink.directory", tmpDir.getPath());

    Configurables.configure(sink, context);

    sink.start();
    sink.stop();
  }

  private void putEvent(final Channel channel, final int idx) throws IOException,
      InterruptedException, EventDeliveryException {
    putEvent(channel, idx, true, Sink.Status.READY);
  }

  private void putEvent(final Channel channel, final int idx, final boolean process,
      final Sink.Status status) throws IOException,
      InterruptedException, EventDeliveryException {
    final Event event = EventBuilder.withBody("Test event " + idx, Charsets.UTF_8);
    channel.put(event);
    if (process) {
      Assert.assertEquals(status, sink.process());
    }
  }

  private String loadRollingFile(final int idx) throws IOException {
    final String files[] = tmpDir.list(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("-" + idx);
      }
    });
    if (files.length == 0) {
      return null;
    }
    return IOUtils.toString(new FileInputStream(new File(tmpDir, files[0])));
  }

  private void shouldRotate() {
    field("shouldRotate").ofType(boolean.class).in(sink).set(true);
  }

  private void disableAutomaticRotation() {
    final Invoker<ScheduledExecutorService> rollServiceInvoker =
        field("rollService").ofType(ScheduledExecutorService.class).in(sink);
    rollServiceInvoker.get().shutdown();
    try {
      rollServiceInvoker.get().awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    final ScheduledExecutorService rollServiceMock = mock(ScheduledExecutorService.class,
        withSettings().stubOnly());
    when(rollServiceMock.isTerminated()).thenReturn(true);
    rollServiceInvoker.set(rollServiceMock);
  }

  @Test
  public void testAppendBatchOne() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    Context context = new Context();

    context.put("sink.directory", tmpDir.getPath());
    context.put("sink.batchSize", "1");

    Configurables.configure(sink, context);

    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();
    disableAutomaticRotation();

    int fileIdx = 1;
    putEvent(channel, 1);
    putEvent(channel, 2);
    shouldRotate();
    Assert.assertEquals("Test event 1\nTest event 2\n", loadRollingFile(fileIdx));
    fileIdx++;
    putEvent(channel, 3);
    shouldRotate();
    Assert.assertEquals("Test event 3\n", loadRollingFile(fileIdx));
    fileIdx++;
    putEvent(channel, 4, false, Sink.Status.READY);
    putEvent(channel, 5, false, Sink.Status.READY);
    putEvent(channel, 6);
    Assert.assertEquals("Test event 4\n", loadRollingFile(fileIdx));
    Assert.assertEquals(Sink.Status.READY, sink.process());
    Assert.assertEquals(Sink.Status.READY, sink.process());
    Assert.assertEquals("Test event 4\nTest event 5\nTest event 6\n", loadRollingFile(fileIdx));
    sink.process();
    shouldRotate();
  }

  @Test
  public void testAppendBatchTwo() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    Context context = new Context();

    context.put("sink.directory", tmpDir.getPath());
    context.put("sink.batchSize", "2");

    Configurables.configure(sink, context);

    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();
    disableAutomaticRotation();

    int fileIdx = 1;
    putEvent(channel, 1, true, Sink.Status.BACKOFF);
    putEvent(channel, 2, true, Sink.Status.BACKOFF);
    shouldRotate();
    Assert.assertEquals("Test event 1\nTest event 2\n", loadRollingFile(fileIdx));
    fileIdx++;
    putEvent(channel, 3, true, Sink.Status.BACKOFF);
    shouldRotate();
    Assert.assertEquals("Test event 3\n", loadRollingFile(fileIdx));
    fileIdx++;
    putEvent(channel, 4, false, Sink.Status.BACKOFF);
    putEvent(channel, 5, true, Sink.Status.READY);
    putEvent(channel, 6, true, Sink.Status.BACKOFF);
    shouldRotate();
    Assert.assertEquals("Test event 4\nTest event 5\nTest event 6\n", loadRollingFile(fileIdx));
  }

}
