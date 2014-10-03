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
package org.apache.flume.sink.hbase;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTestingUtil implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(IntegrationTestHBaseSink.class);

  Configuration conf;
  HBaseAdmin hbaseAdmin;

  public HBaseTestingUtil() throws IOException {
    conf = HBaseConfiguration.create();
    String zkQuorum = System.getProperty("hbase.zookeeper.quorum");
    if (zkQuorum == null) {
      zkQuorum = "localhost";
    }
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.retries", "3");
    hbaseAdmin = new HBaseAdmin(conf);
  }

  @Override
  public void close() {
    try {
      hbaseAdmin.close();
    } catch (IOException ex) {
      logger.warn("Exception while closing hbaseAdmin", ex);
    }
  }

  public HTable createTable(final String tableName, final String columnFamily) throws IOException {
    final HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
    hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
    hbaseAdmin.createTable(hTableDescriptor);
    return new HTable(conf, tableName);
  }

  public void deleteTable(final String tableName) throws IOException {
    try {
      hbaseAdmin.disableTable(tableName);
      hbaseAdmin.deleteTable(tableName);
    } catch (IOException ex) {
      logger.warn("Exception while deleting table", ex);
    }
  }

}
