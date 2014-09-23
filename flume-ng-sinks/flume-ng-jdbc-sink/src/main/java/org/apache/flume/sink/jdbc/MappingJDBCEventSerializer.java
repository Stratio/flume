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

import org.apache.flume.*;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class MappingJDBCEventSerializer implements JDBCEventSerializer {

    private static final Logger log = LoggerFactory.getLogger(MappingJDBCEventSerializer.class);

    private static final String DEFAULT_SQL_DIALECT = "SQL99";
    private static final String DEFAULT_SCHEMA = "public";
    private static final String CONF_DRIVER = "driver";
    private static final String CONF_CONNECTION_STRING = "connectionString";
    private static final String CONF_SQL_DIALECT = "sqlDialect";
    private static final String CONF_SCHEMA = "schema";
    private static final String CONF_TABLE = "table";

    private Table table;
    private SQLDialect sqlDialect;

    public MappingJDBCEventSerializer() {

    }

    public void configure(final Context ctx) {
        final String tableName = ctx.getString(CONF_TABLE);
        if (tableName == null) {
            throw new ConfigurationException(CONF_TABLE + " setting is mandatory");
        }
        final String schemaName = ctx.getString(CONF_SCHEMA, DEFAULT_SCHEMA);
        final String driver = ctx.getString(CONF_DRIVER);
        if (driver == null) {
            throw new ConfigurationException(CONF_DRIVER + " setting is mandatory");
        }
        final String connectionString = ctx.getString(CONF_CONNECTION_STRING);
        if (connectionString == null) {
            throw new ConfigurationException(CONF_CONNECTION_STRING + " setting is mandatory");
        }

        try {
            sqlDialect = SQLDialect.valueOf(
                    ctx.getString(CONF_SQL_DIALECT, DEFAULT_SQL_DIALECT)
                            .toUpperCase(Locale.ENGLISH)
            );
        } catch (IllegalArgumentException ex) {
            throw new ConfigurationException("Invalid sqlDialect", ex);
        }

        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            throw new ConfigurationException(ex);
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionString);
        } catch (SQLException ex) {
            throw new ConfigurationException(ex);
        }

        try {
            Meta meta = DSL.using(connection).meta();

            Schema schema = null;
            for (Schema s : meta.getSchemas()) {
                if (schemaName.equalsIgnoreCase(s.getName())) {
                    schema = s;
                    break;
                }
            }
            if (schema == null) {
                throw new ConfigurationException("Schema not found: " + schemaName);
            }

            for (Table table : schema.getTables()) {
                System.out.println(table.getName());
                if (table.getName().equalsIgnoreCase(tableName)) {
                    this.table = table;
                    break;
                }
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    log.error("Error closing connection", ex);
                }
            }
        }
        if (this.table == null) {
            throw new ConfigurationException("Table not found: " + tableName);
        }
    }

    @Override
    public void insertEvents(final Connection connection, final List<Event> events) {
        final DSLContext dslContext = DSL.using(connection, sqlDialect);

        InsertSetStep insert = dslContext.insertInto(this.table);
        int mappedEvents = 0;
        for (Event event : events) {
            Map<Field, Object> fieldValues = new HashMap<>();
            for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                Field field = null;
                for (Field f: this.table.fields()) {
                    if (f.getName().equalsIgnoreCase(entry.getKey())) {
                        field = f;
                        break;
                    }
                }
                if (field == null) {
                    log.trace("Ignoring field: {}", entry.getKey());
                    continue;
                }
                DataType dataType = field.getDataType();
                fieldValues.put(field, dataType.convert(entry.getValue()));
            }
            if (fieldValues.isEmpty()) {
                log.debug("Ignoring event, no mapped fields.");
            } else {
                mappedEvents++;
                if (insert instanceof InsertSetMoreStep) {
                    insert = ((InsertSetMoreStep) insert).newRecord();
                }
                for (Map.Entry<Field, Object> entry : fieldValues.entrySet()) {
                    insert = insert.set(entry.getKey(), entry.getValue());
                }
            }
        }
        if (insert instanceof InsertSetMoreStep) {
            int result = ((InsertSetMoreStep) insert).execute();
            if (result != mappedEvents) {
                log.warn("Mapped {} events, inserted {}.", mappedEvents, result);
            }
        } else {
            log.debug("No insert.");
        }
    }

    @Override
    public void close() {

    }

}
