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
package org.apache.flume.sink.jdbc;

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Saves Flume events to any database with a JDBC driver. It can operate either
 * with automatic headers-to-tables mapping or with custom SQL queries.
 *
 * Available configuration parameters are:
 *
 * <p><ul>
 * <li><tt>driver</tt> <em>(string, required)</em>: The driver class (e.g.
 *      <tt>org.h2.Driver</tt>, <tt>org.postgresql.Driver</tt>). <strong>NOTE:</strong>
 *      Stratio JDBC Sink does not include any JDBC driver. You must add a JDBC
 *      driver to your Flume classpath.</li>
 * <li><tt>connectionString</tt> <em>(string, required)</em>: A valid
 *      connection string to a database. Check the documentation for your JDBC driver
 *      for more information.</li>
 * <li><tt>sqlDialect</tt> <em>(string, required)</em>: The SQL dialect of your
 *      database. This should be one of the following: <tt>CUBRID</tt>, <tt>DERBY</tt>,
 *      <tt>FIREBIRD</tt>, <tt>H2</tt>, <tt>HSQLDB</tt>, <tt>MARIADB</tt>, <tt>MYSQL</tt>,
 *      <tt>POSTGRES</tt>, <tt>SQLITE</tt>.</li>
 * <li><tt>table</tt> <em>(string)</em>: A table to store your events.
 *      <em>This is only used for automatic mapping.</em></li>
 * <li><tt>sql</tt> <em>(string)</em>: A custom SQL query to use. If specified,
 *      this query will be used instead of automatic mapping. E.g.
 *      <tt>INSERT INTO tweets (text, num_hashtags, timestamp) VALUES (${body:string}, ${header.numberOfHashtags:integer}, ${header.date:timestamp})</tt>.
 *      Note the variable format: the first part is either <tt>body</tt> or
 *      <tt>header.yourHeaderName</tt> and then the SQL type.</li>
 * <li><tt>batchSize</tt> <em>(integer)</em>: Number of events that will be grouped
 *      in the same query and transaction. Defaults to <tt>20</tt>.</li>
 * </ul></p>
 *
 */
public class JDBCSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSink.class);

    private static final int DEFAULT_BATCH_SIZE = 20;
    private static final String CONF_DRIVER = "driver";
    private static final String CONF_CONNECTION_STRING = "connectionString";
    private static final String CONF_BATCH_SIZE = "batchSize";
    private static final String CONF_SERIALIZER = "serializer";

    private static final String MAPPING_SERIALIZER_NAME = "MAPPING";
    private static final String TEMPLATE_SERIALIZER_NAME = "TEMPLATE";


    private Connection connection;
    private String connectionString;
    private Driver driver;
    private SinkCounter sinkCounter;
    private int batchSize;
    private JDBCEventSerializer serializer;
    private List<Event> events;

    public JDBCSink() {
        super();
    }

    @Override
    public void configure(Context context) {
        final String driverName = context.getString(CONF_DRIVER);
        connectionString = context.getString(CONF_CONNECTION_STRING);

        batchSize = context.getInteger(CONF_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw new ConfigurationException(CONF_BATCH_SIZE + " must be greater than 0");
        }
        events = new ArrayList<>(batchSize);

        try {
            driver = (Driver) Class.forName(driverName).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            throw new ConfigurationException(ex);
        }

        try {
            getConnection();
        } catch (SQLException ex) {
            throw new ConfigurationException(ex);
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new ConfigurationException(ex);
        }

        String serializerName = context.getString(CONF_SERIALIZER);
        if (serializerName == null) {
            throw new ConfigurationException(CONF_SERIALIZER + " setting is mandatory");
        }
        if (MAPPING_SERIALIZER_NAME.equals(serializerName.toUpperCase(Locale.ENGLISH))) {
            serializerName = MappingJDBCEventSerializer.class.getName();
        } else if (TEMPLATE_SERIALIZER_NAME.equals(serializerName.toUpperCase(Locale.ENGLISH))) {
            serializerName = TemplateJDBCEventSerializer.class.getName();
        }
        try {
            serializer = (JDBCEventSerializer) Class.forName(serializerName).newInstance();
        } catch (ClassNotFoundException ex) {
            throw new ConfigurationException(CONF_SERIALIZER + " setting is not found", ex);
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new ConfigurationException(CONF_SERIALIZER + " setting is not valid", ex);
        } catch (ClassCastException ex) {
            throw new ConfigurationException(serializerName + " does not implement " + JDBCEventSerializer.class.getCanonicalName(), ex);
        }
        final Context serializerContext = new Context(context.getSubProperties(CONF_SERIALIZER));
        serializer.configure(serializerContext);

        this.sinkCounter = new SinkCounter(this.getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        Transaction transaction = this.getChannel().getTransaction();
        try {
            transaction.begin();

            getConnection();

            takeEventsFromChannel(getChannel());

            status = Status.READY;
            if (!events.isEmpty()) {
                if (events.size() == this.batchSize) {
                    this.sinkCounter.incrementBatchCompleteCount();
                } else {
                    this.sinkCounter.incrementBatchUnderflowCount();
                }

                try {
                    this.serializer.insertEvents(connection, events);
                } catch (Exception ex) {
                    throw new EventDeliveryException(ex);
                }

                connection.commit();

                this.sinkCounter.addToEventDrainSuccessCount(events.size());
            } else {
                this.sinkCounter.incrementBatchEmptyCount();
            }


            transaction.commit();
            status = Status.READY;
        } catch (Exception ex) {
            final String errorMsg = "Failed to publish events";
            logger.error(errorMsg, ex);
            rollbackConnection();
            status = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    logger.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
            events.clear();
        }
        return status;
    }

    @Override
    public synchronized void start() {
        this.sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        this.sinkCounter.stop();
        super.stop();
    }

    private void rollbackConnection() {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException ex) {
            logger.error("Exception on rollback", ex);
        }
    }

    private boolean checkConnection() {
        if (connection == null) {
            return false;
        }
        try {
            connection.createStatement().execute("SELECT 1 FROM dual");
        } catch (SQLException ex) {
            closeConnection();
            return false;
        }
        return true;
    }

    private void closeConnection() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException ex) {
            logger.warn("Exception while closing connection", ex);
        }
    }

    private Connection getConnection() throws SQLException {
        if (!checkConnection()) {
            connection = driver.connect(connectionString, new Properties());
        }
        return connection;
    }


    private void takeEventsFromChannel(Channel channel) {
        events.clear();
        for (int i = 0; i < batchSize; i++) {
            this.sinkCounter.incrementEventDrainAttemptCount();
            final Event event = channel.take();
            if (event == null) {
                break;
            }
            events.add(event);
        }
    }

}